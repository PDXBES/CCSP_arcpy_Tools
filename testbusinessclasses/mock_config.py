import mock
from businessclasses.config import Config

class MockConfig():
    def __init__(self):
        self.config = mock.MagicMock(Config)
        self.config.test_flag = "TEST"
        self.config.model_tracking_sde_path = "model_tracking_sde_path"


