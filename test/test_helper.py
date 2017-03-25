import os

def from_fixture(fixture_name):
    with open(fixture_path(fixture_name), 'r+b') as fixture:
        return fixture.readlines()

def fixture_path(fixture_name):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    return "{0}/fixtures/{1}".format(base_dir, fixture_name)
