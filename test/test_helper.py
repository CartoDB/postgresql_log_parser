import os

def from_fixture(fixture_name):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    with open("{0}/fixtures/{1}".format(base_dir, fixture_name), 'r') as fixture:
        return fixture.readlines()
