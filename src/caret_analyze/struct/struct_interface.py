class NodeStructInterface(metaclass=ABCMeta):

    @property
    @abstractmethod
    def node_name(self) -> str:
        pass

    @property
    @abstractmethod
    def callbacks(self) -> Optional[CallbacksStructInterface]:
        pass

    @property
    @abstractmethod
    def variable_passings(self) -> Optional[VariablePassingsStructInterface]:
        pass

    @property
    @abstractmethod
    def node_inputs(self) -> Sequence[NodeInputType]:
        pass

    @property
    @abstractmethod
    def node_outputs(self) -> Sequence[NodeOutputType]:
        pass

    def get_node_input(self) -> NodeInputType:
        raise NotImplementedError('')

    def get_node_output(self) -> NodeOutputType:
        raise NotImplementedError('')

    @property
    @abstractmethod
    def publishers(self) -> Optional[PublishersStructInterface]:
        pass

    @property
    @abstractmethod
    def tf_broadcaster(self) -> Optional[TransformBroadcasterStructInterface]:
        pass

    @property
    @abstractmethod
    def subscriptions(self) -> Optional[SubscriptionsStructInterface]:
        pass

    @property
    @abstractmethod
    def tf_buffer(self) -> Optional[TransformBufferStructInterface]:
        pass
