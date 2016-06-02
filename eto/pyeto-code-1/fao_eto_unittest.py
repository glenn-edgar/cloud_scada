"""
Unit test script for fao_eto.py
To do
-----
Find test values for hargreaves_ETo()
"""

__author__ = "Mark L.A. Richards <m.richards@REMOVETHISabdn.ac.uk>"
__date__ = "25/11/2010"

import unittest
import fao_eto as fao

class TestGlobalFunctions(unittest.TestCase):

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testatmos_pres(self):
        # Test based on example 2, p.63 of FAO paper
        P = fao.atmos_pres(1800)
        self.assertAlmostEqual(P, 81.8, 1)

    def testea_from_tmin(self):
        # Test based on example 20, p.121 of FAO paper
        ea = fao.ea_from_tmin(14.8)
        self.assertAlmostEqual(ea, 1.68, 2)

    def testea_from_rhmin_rhmax(self):
        # Test based on example 5, p.72 of FAO paper
        ea = fao.ea_from_rhmin_rhmax(2.064, 3.168, 54, 82)
        self.assertAlmostEqual(ea, 1.70, 2)

    def testea_from_rhmax(self):
        ea = fao.ea_from_rhmax(2.064, 82)
        self.assertAlmostEqual(ea, 1.69, 2)

    def testea_from_rhmean(self):
        # Test based on FAO example 5
        ea = fao.ea_from_rhmean(2.064, 3.168, (82+54)/2.0)
        self.assertAlmostEqual(ea, 1.78, 2)

    def testea_from_tdew(self):
        # Test based on example 20, p.121 of FAO paper
        ea = fao.ea_from_tdew(14.8)
        self.assertAlmostEqual(ea, 1.68, 2)

    def testea_from_twet_tdry(self):
        # Test based on example 4, p.70 of FAO paper
        ea = fao.ea_from_twet_tdry(19.5, 25.6, 2.267, 0.000662*87.9)
        self.assertAlmostEqual(ea, 1.91, 2)

    def testclear_sky_rad(self):
        # Test based on example 18, p.115 of FAO paper
        csr = fao.clear_sky_rad(100, 41.09)
        self.assertAlmostEqual(csr, 30.90, 2)

    def testdaily_mean_t(self):
        tmean = fao.daily_mean_t(10, 20)
        self.assertEqual(tmean, 15.0)

    def testdaily_soil_heat_flux(self):
        # Have not found data to test against yet
        #shf = fao.daily_soil_heat_flux(t_cur, t_prev, delta_t, soil_heat_cap=2.1, delta_z=0.10)
        pass

    def testdaylight_hours(self):
        # Test based on example 9, p.83 of FAO paper
        dh = fao.daylight_hours(1.527)
        self.assertAlmostEqual(dh, 11.7, 1)

    def testdelta_sat_vap_pres(self):
        # Test based on example 17, p.111 of FAO paper
        dh = fao.delta_sat_vap_pres(30.2)
        self.assertAlmostEqual(dh, 0.246, 3)

    def testet_rad(self):
        # Test based on example 8, p.80 of FAO paper
        etrad = fao.et_rad(-20, 0.120, 1.527, 0.985)
        self.assertAlmostEqual(etrad, 32.2, 1)

    def testhargreaves_ETo(self):
        # Have not yet found data to test against yet
        #ETo = fao.hargreaves_ETo(tmin, tmax, tmean, Ra)
        #self.assertAlmostEqual(ETo, 32.2, 1)
        pass

    def testinv_rel_dist_earth_sun(self):
        # Test based on example 8, p.80 of FAO paper
        irl = fao.inv_rel_dist_earth_sun(246)
        self.assertAlmostEqual(irl, 0.985, 3)

    def testmean_es(self):
        # Test based on example 3, p.69 of FAO paper
        mean_es = fao.mean_es(15, 24.5)
        self.assertAlmostEqual(mean_es, 2.39, 2)

    def testmonthly_soil_heat_flux(self):
        # Test based on example 13, p.90 of FAO paper
        shf = fao.monthly_soil_heat_flux(14.1, 18.8)
        self.assertAlmostEqual(shf, 0.33, 2)

    def testmonthly_soil_heat_flux2(self):
        # Test based on approximate value expected from
        # example 13, p.90 of FAO paper
        shf = fao.monthly_soil_heat_flux2(14.1, 16.1)
        self.assertAlmostEqual(shf, 0.33, 1)

    def testnet_out_lw_rad(self):
        # Test based on example 11, p.87 of FAO paper
        lwrad = fao.net_out_lw_rad(19.1, 25.1, 14.5, 18.8, 2.1)
        self.assertAlmostEqual(lwrad, 3.5, 1)

    def testnet_rad(self):
        # Test based on example 16, p.99 of FAO paper
        net_rad = fao.net_rad(16.9, 3.0)
        self.assertAlmostEqual(net_rad, 13.9, 1)

    def testnet_in_sol_rad(self):
        # Test based on example 12, p.87 of FAO paper
        # Note, there seems to be a rounding error in the answer given
        # in the FAO paper!
        rad = fao.net_in_sol_rad(14.5)
        self.assertAlmostEqual(rad, 11.1, 0)

    def testpenman_monteith_ETo(self):
        # Test based on example 17, p.110 (monthly calc) and
        # example 18, p.113 (daily calc) of FAO paper
        # Monthly ETo:
        # Note, due to rounding errors in the FAO's example we can only
        # test to 1 decimal place here!
        ETo = fao.penman_monteith_ETo(14.33, 30.2, 2.0, 4.42, 2.85, 0.246, 0.0674, shf=0.14)
        self.assertAlmostEqual(ETo, 5.72, 1)
        # Daily ETo:
        # (Rn, t, ws, es, ea, delta_es, psy, shf=0.0)
        ETo = fao.penman_monteith_ETo(13.28, 16.9, 2.078, 1.997, 1.409, 0.122, 0.0666)
        self.assertAlmostEqual(ETo, 3.9, 1)

    def testpsy_const(self):
        # Test based on example 2, p.63 of FAO paper
        psy = fao.psy_const(81.8)
        self.assertAlmostEqual(psy, 0.054, 3)

    def testpsy_const_of_psychrometer(self):
        # Test based on example 2, p.63 of FAO paper
        ea = 2.267 - fao.psy_const_of_psychrometer(1, 87.9) * (25.6 - 19.5)
        self.assertAlmostEqual(ea, 1.91, 2)

    def testrad2equiv_evap(self):
        # Test based on example 13, p.90 of FAO paper
        evap = fao.rad2equiv_evap(0.33)
        self.assertAlmostEqual(evap, 0.13, 2)

    def testrh_from_ea_es(self):
        rh = fao.rh_from_ea_es(50, 100)
        self.assertAlmostEqual(rh, 50.0, 1)

    def testsol_dec(self):
        # Test based on example 8, p.80 of FAO paper
        sol_dec = fao.sol_dec(246)
        self.assertAlmostEqual(sol_dec, 0.120, 3)

    def testsol_rad_from_sun_hours(self):
        # Test based on example 10, p.84 of FAO paper
        solrad = fao.sol_rad_from_sun_hours(10.9, 7.1, 25.1)
        self.assertAlmostEqual(solrad, 14.45, 2)

    def testsol_rad_from_t(self):
        # Test based on example 15, p.98 of FAO paper
        solrad = fao.sol_rad_from_t(40.6, 50.0, 14.8, 26.6, coastal=False)
        self.assertAlmostEqual(solrad, 22.3, 1)
        # Test that the clear sky radiation constraint is working:
        solrad = fao.sol_rad_from_t(40.6, 20.0, 14.8, 26.6, coastal=False)
        self.assertAlmostEqual(solrad, 20.0, 1)

    def testsol_rad_island(self):
        # No example in FAO paper so have just done a visual check!
        solrad = fao.sol_rad_island(50.0)
        self.assertAlmostEqual(solrad, 31.0, 1)

    def testsunset_hour_angle(self):
        # Test based on example 8, p.80 of FAO paper
        sha = fao.sunset_hour_angle(-20, 0.120)
        self.assertAlmostEqual(sha, 1.527, 3)

    def testwind_speed_2m(self):
        # Test based on example 14, p.92 of FAO paper
        ws = fao.wind_speed_2m(3.2, 10)
        self.assertAlmostEqual(ws, 2.4, 1)

if __name__ == '__main__':
    unittest.main()
