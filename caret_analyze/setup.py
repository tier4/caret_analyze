from setuptools import find_packages
from setuptools import setup

package_name = 'caret_analyze'

setup(
    name=package_name,
    version='0.0.0',
    packages=find_packages(exclude=['test']),
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='hsgwa',
    maintainer_email='hasegawa@isp.co.jp',
    description="CARET\'s tools for analyzing trace results",
    license='Apache License 2.0',
    tests_require=['pytest'],
    entry_points={
        'console_scripts': [f'caret_create = {package_name}.cli:caret_create'],
    },
)
