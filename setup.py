from setuptools import setup

package_name = "trace_analysis"

setup(
    name=package_name,
    version="0.0.0",
    packages=[package_name],
    data_files=[
        ("share/ament_index/resource_index/packages",
         ["resource/" + package_name]),
        ("share/" + package_name, ["package.xml"]),
    ],
    install_requires=["setuptools"],
    zip_safe=True,
    maintainer="hsgwa",
    maintainer_email="hasegawa@isp.co.jp",
    description="TODO: Package description",
    license="Apache License 2.0",
    tests_require=["pytest"],
    entry_points={
        "console_scripts": [
            "talker = trace_analysis.publisher_member_function:main",
            "listener = trace_analysis.subscriber_member_function:main",
        ],
    },
)
