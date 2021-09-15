from setuptools import setup

package_name = 'ros2caret'

setup(
    name=package_name,
    version='0.1.0',
    packages=[package_name],
    data_files=[
        ('share/ament_index/resource_index/packages',
            ['resource/' + package_name]),
        ('share/' + package_name, ['package.xml']),
    ],
    install_requires=['ros2cli'],
    zip_safe=True,
    maintainer='hsgwa',
    maintainer_email='hasegawa@isp.co.jp',
    description='TODO: Package description',
    license='Apache License 2.0',
    tests_require=['pytest'],
    entry_points={
        'ros2cli.command': [
            'caret = ros2caret.command.caret:CaretCommand',
        ],
        'ros2cli.extension_point': [
            'ros2caret.verb = ros2caret.verb:VerbExtension'
        ],
        'ros2caret.verb': [
            'architecture = ros2caret.verb.architecture:ArchitectureVerb',
            'callback_graph = ros2caret.verb.callback_graph:CallbackGraphVerb',
            'chain_latency = ros2caret.verb.chain_latency:ChainLatencyVerb',
            'message_flow = ros2caret.verb.message_flow:MessageFlowVerb',
        ]
    },
)
