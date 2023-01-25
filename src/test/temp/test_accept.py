# 注意：このファイルは開発用の一時的なテストコード
# 最終的なコミットには含めないこと。

class TestAccept:

    def test_create_architecture(self):
        from caret_analyze import Architecture

        file = '/home/hasegawa/ros2_caret_ws/caret_develop/autoware_duplicated/autoware_launch_trace_20221222-181030'

        arch = Architecture('lttng', file)
        arch.export('tmp.yaml', force=True)

    def test_read_architecture(self):
        from caret_analyze import Architecture
        file = 'tmp.yaml'
        duplicated_file = 'tmp_duplicated.yaml'
        arch = Architecture('yaml', file)
        arch.export(duplicated_file, force=True)

        with open(file, 'r') as f:
            file_content = f.read()

        with open(duplicated_file, 'r') as f:
            duplicated_file_content = f.read()

        # Failするが、自動で割り当てる箇所の違いのみになっている。OK.
        # Failそのものの気持ち悪さはあるが後回しにして良い。
        assert file_content == duplicated_file_content
        # 同じく、ノードとsubでも、コールバックのシンボル名が異なりsub のorderがゼロのケースがある。
        # コールバック側のorderは別途算出指せる必要がある。


    def test_get_communication(self):
        from caret_analyze import Architecture
        file = 'tmp.yaml'
        arch = Architecture('yaml', file)

        src_node = '/localization/pose_twist_fusion_filter/stop_filter'
        dst_node = '/planning/mission_planning/mission_planner'    # ERROR
        topic_name = '/localization/kinematic_state'
        # Applicadtionクラスの前に、Architectureクラスでテスト。
        comm = arch.get_communication(src_node, dst_node, topic_name)
        comms = arch.communications
        comms = list(filter(lambda x: x.topic_name == topic_name, comms))
        comms = list(filter(lambda x: x.publish_node_name == src_node, comms))
        comms = list(filter(lambda x: x.subscribe_node_name == dst_node, comms))
        assert len(comms) == 2
        # construction_orderだけが異なる２つのCommunicationが見つかる。

    def test_path_search(self):

        from caret_analyze import Architecture
        file = 'tmp.yaml'
        arch = Architecture('yaml', file)

        src_node = '/localization/pose_twist_fusion_filter/stop_filter'
        dst_node = '/planning/mission_planning/mission_planner'

        target_path_list = arch.search_paths(src_node, dst_node, max_node_depth=5)
        []
