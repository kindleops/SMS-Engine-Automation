# sms/__init__.py
"""
Package init kept side-effect free. Do NOT import submodules from here.
This avoids boot-time crashes if env/config isn’t ready yet.
"""
__all__: list[str] = []