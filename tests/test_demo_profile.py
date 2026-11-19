import math
import unittest


def eval_demo_kinematics(t_mod: float):
    """Mirror of demo_telemetry_loop kinematics in main.py for deterministic verification."""
    if t_mod < 150.0:
        return 0.0, 0.0, 1.0
    elif t_mod < 152.2:
        tau = t_mod - 150.0
        if tau < 0.35:
            az = 1.0 + 14.0 * (tau / 0.35)
        elif tau < 1.35:
            az = 15.0 - 1.5 * ((tau - 0.35) / 1.0)
        else:
            az = 13.5 - 5.5 * ((tau - 1.35) / 0.85)
        vz = 77.95 * tau
        alt = 0.5 * 77.95 * (tau ** 2)
        return alt, vz, az
    elif t_mod < 169.0:
        tau = t_mod - 152.2
        norm = tau / 16.8
        az = -1.8 * ((1.0 - norm) ** 1.35)
        vz = max(0.0, 156.1 * (1.0 - norm))
        alt = 188.6 + (1500.0 - 188.6) * (1.0 - (1.0 - norm) ** 2)
        return alt, vz, az
    elif t_mod < 199.0:
        tau = t_mod - 169.0
        az = (1.0 + 2.5 * math.sin(math.pi * (tau / 1.0))) if tau < 1.0 else 1.0
        vz = -42.7
        alt = max(220.0, 1500.0 - (1280.0 / 30.0) * tau)
        return alt, vz, az
    elif t_mod < 239.0:
        tau = t_mod - 199.0
        norm = min(1.0, tau / 40.0)
        if tau < 1.2:
            az = 1.0 + 1.4 * math.sin(math.pi * (tau / 1.2))
        elif tau >= 39.4:
            az = 1.0 + 0.6 * math.sin(math.pi * ((tau - 39.4) / 0.6))
        else:
            az = 1.0
        alt = max(0.0, 220.0 * ((1.0 - norm) ** 1.15))
        vz = -6.3 * ((1.0 - norm) ** 0.15)
        return alt, vz, az
    return 0.0, 0.0, 1.0


class TestDemoFlightProfile(unittest.TestCase):
    def test_profile_bounds_and_apogee(self):
        samples = [eval_demo_kinematics(150.0 + i * 0.1) for i in range(950)]
        alts = [s[0] for s in samples]
        vzs = [s[1] for s in samples]
        azs = [s[2] for s in samples]

        self.assertAlmostEqual(max(azs), 15.0, delta=0.2)
        self.assertGreaterEqual(min(azs), -2.0)
        self.assertLess(min(azs), -1.0)
        self.assertAlmostEqual(max(alts), 1500.0, delta=5.0)
        # Soft landing velocity just before touchdown (t=238.5s)
        _, vz_touchdown, _ = eval_demo_kinematics(238.5)
        self.assertLess(abs(vz_touchdown), 7.0)
        self.assertGreater(abs(vz_touchdown), 0.1)


if __name__ == "__main__":
    unittest.main()
