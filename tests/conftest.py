# conftest.py
def pytest_addoption(parser):
    parser.addoption(
        "--input", action="store", default=None, help="Name of the input to select test data"
    )
