from setuptools import find_packages, setup

package_name = 'caret_analyze'

setup(
    name=package_name,
    version='0.2.2',
    packages=find_packages(exclude=['test']),
    package_data={package_name: ['log_config.yaml']},
    data_files=[
        ('share/ament_index/resource_index/packages', ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['setuptools'],
    zip_safe=True,
    maintainer='ymski, isp-uetsuki',
    maintainer_email='yamasaki@isp.co.jp, uetsuki@isp.co.jp',
    description="CARET\'s tools for analyzing trace results",
    license='Apache License 2.0',
    extras_require={
        'test': [
            'pytest',
        ],
    },
    entry_points={
    },
)
