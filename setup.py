from setuptools import setup
from setuptools import find_packages

def readme():
    with open('README.rst') as f:
        return f.read()

with open('requirements.txt', 'r') as f:
    requirements = f.readlines()

setup(
        name='migrate-to-azure',
        version='0.1',
        description='Migrate buckets from s3 to azure as blob containers',
        long_description=readme(),
        url='https://github.com/jeslyvarghese/aws-to-azure-migrator',
        author='Jesly Varghese',
        author_email='jesly.varghese@gmail.com',
        license='MIT',
        packages=['migrator', 'migrator.migrations', 'migrator.providers'],
        zip_safe=False,
        include_package_data=True,
        install_requires=requirements,
        entry_points={'console_scripts': ['migrate-to-azure=migrator.commandline:run']})
