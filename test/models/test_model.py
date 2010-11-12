import polydorus
from settings import Settings

class TestModel(polydorus.Model):
	
    def __init__(self, *args, **kwargs):
        super(TestModel, self).__init__(Settings.client, *args, **kwargs)

    def test(self):
        print self._client