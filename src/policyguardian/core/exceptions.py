from __future__ import annotations


class PolicyGuardianError(Exception):
    pass


class ConfigurationError(PolicyGuardianError):
    pass


class ValidationError(PolicyGuardianError):
    pass


class UnsupportedFormatError(PolicyGuardianError):
    pass


class CheckRegistrationError(PolicyGuardianError):
    pass 