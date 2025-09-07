# multidb_project/routers.py
import itertools
from django.conf import settings
import threading

# Thread-local storage for replication state
_replication_state = threading.local()


class MultiDBRouter:
    """
    Recursion-safe multi-DB router with round-robin reads.
    Tracks currently replicating databases to prevent infinite recursion.
    """

    @staticmethod
    def start_replication(alias):
        if not hasattr(_replication_state, "replicating"):
            _replication_state.replicating = set()
        _replication_state.replicating.add(alias)

    @staticmethod
    def stop_replication(alias):
        _replication_state.replicating.remove(alias)

    @staticmethod
    def is_replicating(alias):
        return (
            hasattr(_replication_state, "replicating")
            and alias in _replication_state.replicating
        )

    """
    A database router that:
      - Routes all writes to default unless specified
      - Distributes reads across all databases (load balancing) unless specified
      - Prevents cross-database relations
      - Allows migrations everywhere
    """

    def __init__(self):
        # Round-robin iterator over available DBs
        self.read_dbs = list(settings.DATABASES.keys())
        self.read_cycle = itertools.cycle(self.read_dbs)

    def db_for_read(self, model, **hints):
        """Point all reads to Round-robin between all unless specified."""
        if hints.get("database"):
            return hints["database"]
        """Round-robin between all databases for reads."""
        db = next(self.read_cycle)
        print(f"[Router] Read from {db} for {model.__name__}")
        return db

    def db_for_write(self, model, **hints):
        """Point all writes to default unless specified."""
        db = hints.get("database", "default")
        print(f"[Router] Write to {db} for {model.__name__}")
        return db

    def allow_relation(self, obj1, obj2, **hints):
        """
        Allow relation only if both objects are in the same DB.
        Prevents recursion and cross-DB integrity issues.
        """
        if obj1._state.db and obj2._state.db:
            return obj1._state.db == obj2._state.db
        return None

    def allow_migrate(self, db, app_label, model_name=None, **hints):
        """Allow migrations on all DBs (or restrict if needed)."""
        return True


# class MultiDBRouter:
#     """
#     A router to control all database operations on models in our project.
#     """
#
#     def db_for_read(self, model, **hints):
#         """Point all reads to default unless specified."""
#         return hints.get("database", "default")
#
#     def db_for_write(self, model, **hints):
#         """Point all writes to default unless specified."""
#         return hints.get("database", "default")
#
#     def allow_relation(self, obj1, obj2, **hints):
#         """
#         Allow relations if both objects are in the same DB.
#         Prevents recursion and cross-DB integrity issues.
#         """
#         if obj1._state.db and obj2._state.db:
#             return obj1._state.db == obj2._state.db
#         return None
#
#     def allow_migrate(self, db, app_label, model_name=None, **hints):
#         """Allow all migrations on all databases (or restrict if needed)."""
#         return True


# class MultiDBRouter:
#     """
#     A database router that:
#       - Routes all writes to default
#       - Distributes reads across all databases (load balancing)
#       - Prevents cross-database relations
#       - Allows migrations everywhere
#     """
#
#     def __init__(self):
#         # Round-robin iterator over available DBs
#         self.read_dbs = list(settings.DATABASES.keys())
#         self.read_cycle = itertools.cycle(self.read_dbs)
#
#     def db_for_read(self, model, **hints):
#         """Round-robin between all databases for reads."""
#         return next(self.read_cycle)
#
#     def db_for_write(self, model, **hints):
#         """All writes go to default."""
#         return "default"
#
#     def allow_relation(self, obj1, obj2, **hints):
#         """
#         Allow relation only if both objects are in the same DB.
#         Prevents recursion and cross-DB integrity issues.
#         """
#         if obj1._state.db and obj2._state.db:
#             return obj1._state.db == obj2._state.db
#         return None
#
#     def allow_migrate(self, db, app_label, model_name=None, **hints):
#         """Allow migrations on all DBs (or restrict if needed)."""
#         return True
