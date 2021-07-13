from setuptools import setup, find_packages, find_namespace_packages

INTERNAL_REQUIRES = []
EXTERNAL_REQUIRES = ['fsspec']

INSTALL_REQUIRES = INTERNAL_REQUIRES + EXTERNAL_REQUIRES


def setup_package():
    setup(
        name='filesystem_core',
        author='Linzhou ZHONG',
        url='https://github.com/linzhou-zhong',
        install_requires=INSTALL_REQUIRES,
        packages=find_packages()
    )


if __name__ == '__main__':
    setup_package()