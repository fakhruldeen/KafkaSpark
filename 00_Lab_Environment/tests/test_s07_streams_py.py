from python.s07_streams_py import KeyedAggregate, run_topology


def events():
    return [
        {"customer_id": "C1", "amount": 300},
        {"customer_id": "C2", "amount": 50},   # dropped by the filter
        {"customer_id": "C1", "amount": 120},
        {"customer_id": "C2", "amount": 400},
    ]


def test_topology_filters_and_aggregates():
    store = KeyedAggregate(lambda acc, v: acc + v)
    passed = run_topology(events(), store, min_amount=100)
    assert passed == 3
    assert store.state == {"C1": 420.0, "C2": 400.0}


def test_changelog_has_one_entry_per_update():
    store = KeyedAggregate(lambda acc, v: acc + v)
    run_topology(events(), store, min_amount=100)
    assert len(store.changelog) == 3
    assert store.changelog[-1] == ("C2", 400.0)


def test_restore_from_changelog_rebuilds_the_same_state():
    store = KeyedAggregate(lambda acc, v: acc + v)
    run_topology(events(), store, min_amount=100)
    restored = KeyedAggregate.restore(store.changelog, lambda acc, v: acc + v)
    assert restored.state == store.state


def test_max_aggregate_is_a_different_table():
    store = KeyedAggregate(lambda acc, v: max(acc, v))
    run_topology(events(), store, min_amount=0)
    assert store.state["C2"] == 400.0
