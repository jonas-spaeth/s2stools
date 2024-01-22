from setuptools import setup


def get_version():
    with open("VERSION") as f:
        return f.read().strip()


if __name__ == "__main__":
    setup(version=get_version())
