from .mod3 import baz
from ..subpkg1.mod1 import foo

__all__ = ['baz']

print("Running pkg.subpkg2.__init__.py")