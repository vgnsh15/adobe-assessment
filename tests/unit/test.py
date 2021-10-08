import unittest
import pandas as pd
from pandas.util.testing import assert_frame_equal # <-- for testing dataframes
from adobe-assessment.python.purchase_transaction import Event


class TestSum(unittest.TestCase):
    def test_list_int(self):
        """
        Test that it can sum a list of integers
        """
        data={'hit_time_gmt':['1254033280'],'date_time':['2009-09-27 06:34:40'],'user_agent':['Mozilla/5.0 (Windows; U; Windows NT 5.1; en-US; rv:1.9.0.10) Gecko/2009042316 Firefox/3.0.10'],'ip':['67.98.123.1'],'event_list':[2,100,101,3,1],'geo_city':['Salem'],'geo_region':['OR'],'geo_country':['US'],'pagename':['Home'],'page_url':['http://www.esshopzilla.com'],'product_list':['Electronics;Ipod - Nano - 8GB;1;190; ,Electronics;Ipad - Nano - 8GB;1;500; '],'referrer':['http://www.google.com/search?q=Ipad&client=firefox-a&rls=org.mozilla%3Aen-US%3Aofficial&hs=ZzP&q=Ipod&aq=f&oq=&aqi=']}
        # Create DataFrame
        df = pd.DataFrame(data)
        data={'search_key':['Ipad'],'search_domain':['https://google.com'],'revenue':['690']}

        events = Event()
        result=events.transaction_events(df)
        self.assertEqual(result, 6)
        assert_frame_equal(result, data)

if __name__ == '__main__':
    unittest.main()
