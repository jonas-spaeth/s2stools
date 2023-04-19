import unittest
from s2stools import plot


class MyTestCase(unittest.TestCase):
    def test_themes(self):
        plot.themes.beach_towel()


if __name__ == '__main__':
    unittest.main()
