import unittest

from adobe-assessment/python/purchase_transaction import Event


class TestSum(unittest.TestCase):
    def test_list_int(self):
        """
        Test that it can sum a list of integers
        """
        data = [1, 2, 3]
        result = Event()
        self.assertEqual(result., 6)

if __name__ == '__main__':
    unittest.main()
