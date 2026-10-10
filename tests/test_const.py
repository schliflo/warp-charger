from warp_charger import const


def test_meter_values_consistent() -> None:
    assert const.METER_VALUES[74].key == "power_active_l_sum_im_ex_diff"
    assert const.METER_VALUES[209] == const.MeterValue("energy_active_l_sum_import", "kWh", 3)
    assert const.METER_VALUES[1].unit == "V"
    keys = [m.key for m in const.METER_VALUES.values()]
    assert len(keys) == len(set(keys))
    assert {m.unit for m in const.METER_VALUES.values()} <= {"V", "A", "W", "kWh", "Hz", "%"}


def test_enums() -> None:
    assert 1 not in const.ERROR_STATE
    assert const.CHARGE_MODE[const.CHARGE_MODE_DEFAULT] == "default"
    assert len(const.DURATION_LIMITS) == 11
