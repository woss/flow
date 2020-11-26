use protocol::protocol;
use tinyvec::{array_vec, ArrayVec};

/// RouteConstraint constrains how a request may be dispatched among members.
pub enum RouteConstraint<'a> {
    /// Dispatch to the default service address.
    Default,
    /// Dispatch to the closest member of the item's route.
    ItemAny(&'a str),
    /// Dispatch to the primary member of the item's route.
    ItemPrimary(&'a str),
    /// Dispatch to the closest member of the route.
    Route(&'a protocol::Route),
    /// Dispatch to a fixed endpoint.
    Fixed(&'a str),
}

impl<'a> RouteConstraint<'a> {
    /// Pick endpoints to which this constrained route may be addressed.
    /// Depending on the constraint, there may be exactly one endpoint
    /// which matches.
    ///
    /// For other scenarios (eg, we can select allow any replica of a
    /// current topology), we have some flexibility in candidate
    /// selection, and seek to pick candidates which are "closest"
    /// to our current |zone|.
    ///
    /// For example, consider a |zone| of 'gcp-us-central1-a',
    /// and Route with members having zones like: [
    ///      'aws-east1-b',
    ///      'gcp-eu-west1-a',
    ///      'gcp-us-central1-b',
    ///      'gcp-us-central1-c',
    /// ]
    ///
    /// Though there isn't an exact match, we certainly want to pick
    /// among the 'gcp-us-central1-*" zones. If we *had* an exact zone
    /// match, we'd prefer that above all others.
    pub fn pick_candidates<'s, R>(&'s self, route: R, zone: &str) -> ArrayVec<B<'a>>
    where
        R: FnOnce(&'s str) -> Option<&'a protocol::Route>,
    {
        let route = match self {
            RouteConstraint::Default => return array_vec!(B),
            RouteConstraint::ItemAny(key) | RouteConstraint::ItemPrimary(key) => match route(key) {
                // If item cannot be routed, use default service.
                None => return array_vec!(B),
                // If we can pluck out a preferred primary, return it directly.
                Some(rt) if matches!(self, RouteConstraint::ItemPrimary(..)) => {
                    // This will be None if, eg, there is no primary (-1).
                    if let Some(ep) = rt.endpoints.get(rt.primary as usize) {
                        return array_vec!(B => ep.as_str());
                    }
                    rt
                }
                Some(rt) => rt,
            },
            RouteConstraint::Route(rt) => *rt,
            RouteConstraint::Fixed(ep) => {
                return array_vec!(B => *ep);
            }
        };

        // We have a matched Route, but aren't constrained to a specific member.
        // Build a cohort of those members having zones that overlap the most
        // with our own |zone| (in terms of string prefix).
        //
        // In other words, this is using "prefix same-ness" of the zone as
        // a proxy for geographic proximity.
        let mut n = 0;
        let mut picks = ArrayVec::new();

        for (i, (id, ep)) in route.members.iter().zip(route.endpoints.iter()).enumerate() {
            let nn = common_prefix(zone.as_bytes(), id.zone.as_bytes());

            if i == 0 {
                // Initialize with first member.
                picks.push(ep.as_ref());
                n = nn;
            } else if nn > n {
                // If this match is strictly better, it replaces
                // the candidate cohort that we'll consider.
                picks.clear();
                picks.push(ep.as_ref());
                n = nn;
            } else if nn == n {
                // Equally good. Extend the candidate cohort.
                let _ = picks.try_push(route.endpoints[i].as_ref());
            }
        }
        picks
    }
}

#[cfg(test)]
mod test {
    use super::{common_prefix, protocol::Route, RouteConstraint, B};
    use serde_json::json;
    use tinyvec::array_vec;

    #[test]
    fn route_constraint_cases() {
        let fixture = serde_json::from_value::<Route>(json!({
            "members": [
                { "zone": "aws-east1-a", "suffix": "one" },
                { "zone": "aws-east1-b", "suffix": "two" },
                { "zone": "gcp-west2-c", "suffix": "three" },
            ],
            "endpoints": ["http://one/a", "http://two/b", "http://three/c"],
            "primary": 1,
        }))
        .unwrap();

        let bad_fixture = serde_json::from_value::<Route>(json!({
            "members": [
                { "zone": "aws-east1-a", "suffix": "one" },
                { "zone": "aws-east1-b", "suffix": "two" },
                { "zone": "gcp-west2-c", "suffix": "three" },
            ],
            "endpoints": [],
            "primary": -12345,
        }))
        .unwrap();

        let route = |item| match item {
            "hit" => Some(&fixture),
            "bad" => Some(&bad_fixture),
            "miss" => None,
            _ => unreachable!(),
        };

        assert_eq!(
            RouteConstraint::Default.pick_candidates(&route, "aws-east1-a"),
            array_vec!(B),
        );
        // Within a served zone, we prefer the matched endpoint.
        assert_eq!(
            RouteConstraint::ItemAny("hit").pick_candidates(&route, "aws-east1-b"),
            array_vec!(B => "http://two/b"),
        );
        // Within a region, we prefer zones of that region.
        assert_eq!(
            RouteConstraint::ItemAny("hit").pick_candidates(&route, "aws-east1-z"),
            array_vec!(B => "http://one/a", "http://two/b"),
        );
        // Within a cloud, we prefer the same cloud.
        assert_eq!(
            RouteConstraint::ItemAny("hit").pick_candidates(&route, "gcp-something"),
            array_vec!(B => "http://three/c"),
        );
        // We'll traverse whatever we must, to get to the primary if required.
        assert_eq!(
            RouteConstraint::ItemPrimary("hit").pick_candidates(&route, "gcp-something"),
            array_vec!(B => "http://two/b"),
        );
        // Item route isn't known.
        assert_eq!(
            RouteConstraint::ItemAny("miss").pick_candidates(&route, "gcp-something"),
            array_vec!(B),
        );
        assert_eq!(
            RouteConstraint::ItemPrimary("miss").pick_candidates(&route, "gcp-something"),
            array_vec!(B),
        );
        // Route applies prior knowledge of the route to use.
        let rt = RouteConstraint::Route(&fixture);
        assert_eq!(
            rt.pick_candidates(&route, "aws-east1-z"),
            array_vec!(B => "http://one/a", "http://two/b"),
        );
        // Fixed route is fixed.
        assert_eq!(
            RouteConstraint::Fixed("http://fixed").pick_candidates(&route, "aws-east1-b"),
            array_vec!(B => "http://fixed"),
        );

        // We handle a malformed Route by ignoring broken bits.
        assert_eq!(
            RouteConstraint::ItemAny("bad").pick_candidates(&route, "aws-something"),
            array_vec!(B),
        );
        assert_eq!(
            RouteConstraint::ItemPrimary("bad").pick_candidates(&route, "aws-something"),
            array_vec!(B),
        );
        let rt = RouteConstraint::Route(&bad_fixture);
        assert_eq!(rt.pick_candidates(&route, "gcp-something"), array_vec!(B));
    }

    #[test]
    fn test_prefix_lengths() {
        assert_eq!(common_prefix(b"foobar", b"foorab"), 3);
        assert_eq!(common_prefix(b"foobar", b"FOORAB"), 0);
        assert_eq!(common_prefix(b"aws-east1-a", b"aws-east1-a"), 11);
        assert_eq!(common_prefix(b"aws-east1-a", b"aws-east1"), 9);
        assert_eq!(common_prefix(b"aws-east1-a", b"aws-west1"), 4);
    }
}

#[inline]
fn common_prefix(a: &[u8], b: &[u8]) -> usize {
    a.iter()
        .zip(b.iter())
        .enumerate()
        .filter_map(|(i, (a, b))| if a != b { Some(i) } else { None })
        .next()
        .unwrap_or(std::cmp::min(a.len(), b.len()))
}

type B<'a> = [&'a str; 4];
