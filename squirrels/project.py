from . import _environcfg as e, _manifest as m, _seeds as s


class SquirrelsProject:
    def __init__(self, *, auto_reload = False):
        self._auto_reload = auto_reload
        if not self.auto_reload:
            self._env_cfg = e.EnvironConfigIO.load_from_file()
            self._manifest_cfg = m.ManifestIO.load_from_file(self._env_cfg)
            self._seeds = s.SeedsIO.load_files(self._manifest_cfg)
    
    @property
    def auto_reload(self):
        return self._auto_reload
    
    @property
    def env_cfg(self):
        return self._env_cfg if not self.auto_reload else e.EnvironConfigIO.load_from_file()

    @property
    def manifest_cfg(self):
        return self._manifest_cfg if not self.auto_reload else m.ManifestIO.load_from_file(self.env_cfg)