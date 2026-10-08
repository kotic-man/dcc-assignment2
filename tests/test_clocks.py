from clocks import LamportClock


def test_local_event_increments_clock():
    clock = LamportClock("p")
    assert clock.local_event("APPLY", "a") == 1
    assert clock.local_event("APPLY", "b") == 2


def test_receive_takes_max_plus_one():
    clock = LamportClock("p")
    clock.local_event("SEND", "m")           # L=1
    assert clock.receive("m", 10) == 11      # max(1, 10) + 1


def test_receive_with_smaller_value_still_increments():
    clock = LamportClock("p")
    for _ in range(5):
        clock.local_event("SEND", "m")       # L=5
    assert clock.receive("m", 2) == 6        # max(5, 2) + 1