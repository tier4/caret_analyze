# Copyright 2021 Research Institute of Systems Planning, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Optional, List
from graphviz import Digraph
import numpy as np

from ..path import Path
from ..callback import CallbackBase
from ..communication import Communication, VariablePassing


def path_latency(path: Path, granularity: Optional[str] = None, **kwargs):
    granularity = granularity or "callback"
    assert granularity in ["callback", "node", "end-to-end"]

    dot = Digraph()
    dot.engine = kwargs.get("engine", "dot")

    dot.attr("node", shape="box")

    def to_node_paths(path) -> List[Path]:
        callbacks: List[CallbackBase] = []
        paths: List[Path] = []
        for cb, cb_ in zip(path.callbacks[:-1], path.callbacks[1:]):
            callbacks.append(cb)
            if cb.node_name != cb_.node_name:
                paths.append(Path(callbacks, path.communications, path.variable_passings))
                callbacks.clear()
        paths.append(
            Path(callbacks + [path.callbacks[-1]], path.communications, path.variable_passings)
        )
        return paths

    def to_label(latensy):
        label = (
            "min: {:.2f} ms\n".format(np.min(latency * 1.0e-6))
            + "avg: {:.2f} ms\n".format(np.average(latency * 1.0e-6))
            + "max: {:.2f} ms".format(np.max(latency * 1.0e-6))
        )
        return label

    if granularity == "callback":
        for component in path:
            _, latency = component.to_timeseries(remove_dropped=True)
            if isinstance(component, CallbackBase):
                label = f"{component.node_name}\n{component.callback_name}\n"
                label += to_label(latency)
                dot.node(component.unique_name, label=label)
            elif isinstance(component, Communication):
                label = component.topic_name
                label += "\n" + to_label(latency)
                dot.edge(
                    component.callback_from.unique_name,
                    component.callback_to.unique_name,
                    label=label,
                )
            elif isinstance(component, VariablePassing):
                # label = to_label(latency)
                dot.edge(
                    component.callback_from.unique_name,
                    component.callback_to.unique_name,
                    label=label,
                )
    elif granularity == "node":
        node_paths = to_node_paths(path)
        for node_path in node_paths:
            _, latency = node_path.to_timeseries(remove_dropped=True)
            node_name = node_path.callbacks[0].node_name
            label = node_name
            label += "\n" + to_label(latency)
            dot.node(node_name, label=label)
        node_names = [path.callbacks[0].node_name for path in node_paths]

        for comm_path in path.communications:
            if (
                comm_path.callback_from.node_name not in node_names
                or comm_path.callback_to.node_name not in node_names
            ):
                continue
            if comm_path.is_intra_process:
                _, pubsub_latency = comm_path.to_timeseries(remove_dropped=True)
                label = comm_path.topic_name
                label += "\n" + (
                    "min: {:.2f} ms\n".format(np.min(pubsub_latency * 1.0e-6))
                    + "avg: {:.2f} ms\n".format(np.average(pubsub_latency * 1.0e-6))
                    + "max: {:.2f} ms".format(np.max(pubsub_latency * 1.0e-6))
                )
            else:
                _, pubsub_latency = comm_path.to_pubsub_latency().to_timeseries(
                    remove_dropped=True
                )
                _, dds_latency = comm_path.to_dds_latency().to_timeseries(remove_dropped=True)
                label = comm_path.topic_name
                label += "\n" + (
                    "min: {:.2f} ({:.2f}) ms\n".format(
                        np.min(pubsub_latency * 1.0e-6), np.min(dds_latency * 1.0e-6)
                    )
                    + "avg: {:.2f} ({:.2f}) ms\n".format(
                        np.average(pubsub_latency * 1.0e-6), np.min(dds_latency * 1.0e-6)
                    )
                    + "max: {:.2f} ({:.2f}) ms".format(
                        np.max(pubsub_latency * 1.0e-6), np.min(dds_latency * 1.0e-6)
                    )
                )

            dot.edge(
                comm_path.callback_from.node_name,
                comm_path.callback_to.node_name,
                label=label,
            )
    elif granularity == "end-to-end":
        node_paths = to_node_paths(path)

        for node_path in [node_paths[0], node_paths[-1]]:
            _, latency = node_path.to_timeseries(remove_dropped=True)
            node_name = node_path.callbacks[0].node_name
            label = node_name + "\n" + to_label(latency)
            dot.node(node_name, label=label)

        inter_mediate_callbacks = []
        terminal_callbacks = node_paths[0].callbacks + node_paths[-1].callbacks
        for callback in path.callbacks:
            if callback not in terminal_callbacks:
                inter_mediate_callbacks.append(callback)

        path = Path(inter_mediate_callbacks, path.communications, path.variable_passings)
        _, latency = path.to_timeseries(remove_dropped=True)

        start_node_name = node_paths[0].callbacks[0].node_name
        end_node_name = node_paths[-1].callbacks[0].node_name
        dot.edge(start_node_name, end_node_name, label=to_label(latency))

    return dot
