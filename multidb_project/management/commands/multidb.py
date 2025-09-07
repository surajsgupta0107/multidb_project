# multidb_project/management/commands/multidb.py
from django.core.management.base import BaseCommand, CommandError
from django.core.management import call_command
from django.conf import settings
from django.apps import apps
from django.db import connections
import os

"""
python manage.py multidb migrate     # migrate all DBs
python manage.py multidb sync        # sync data from default → others
python manage.py multidb flush       # flush all DBs
python manage.py multidb dump        # dump all DBs into JSON
python manage.py multidb load        # load all DBs from JSON
python manage.py multidb status      # check tables + row counts
python manage.py multidb compare     # compare row counts
"""


class Command(BaseCommand):
    help = "Multi-database management (migrate, sync, flush, dump, load, status, compare)"

    def add_arguments(self, parser):
        subparsers = parser.add_subparsers(dest="subcommand", help="Subcommands")

        # migrate
        subparsers.add_parser("migrate", help="Run migrations on all databases")

        # sync
        sync_parser = subparsers.add_parser("sync", help="Sync data from default DB to all others")
        sync_parser.add_argument(
            "--apps",
            help="Comma-separated list of apps to sync (default: all custom apps)"),
        sync_parser.add_argument(
            "--models",
            help="Comma-separated list of models to sync (e.g. customers.Customer,orders.Order)"),
        sync_parser.add_argument(
            "--exclude",
            help="Comma-separated list of apps or models to exclude (e.g. auth,sessions or customers.Customer)",
        )
        sync_parser.add_argument(
            "--safe", action="store_true",
            help="Use safe mode (update_or_create) instead of wiping tables",
        )

        # flush
        flush_parser = subparsers.add_parser("flush", help="Flush all databases")
        flush_parser.add_argument("--noinput", action="store_true", help="Do not prompt for confirmation")

        # dump
        dump_parser = subparsers.add_parser("dump", help="Dumpdata from all DBs")
        dump_parser.add_argument("--output-dir", default="backups", help="Directory to save dump files")

        # load
        load_parser = subparsers.add_parser("load", help="Loaddata into all DBs")
        load_parser.add_argument("--input-dir", default="backups", help="Directory containing dump files")

        # status
        subparsers.add_parser("status", help="Show table + row counts across all DBs")

        # compare
        compare_parser = subparsers.add_parser("compare", help="Compare row counts across all DBs")
        compare_parser.add_argument("--repair", action="store_true", help="Repair mismatches by syncing from default DB")
        compare_parser.add_argument("--interactive", action="store_true", help="Ask before repairing each mismatch")
        compare_parser.add_argument("--dry-run", action="store_true", help="Show what would be repaired without applying")

    def handle(self, *args, **options):
        subcommand = options.get("subcommand")
        if not subcommand:
            raise CommandError("You must specify a subcommand (migrate|sync|flush|dump|load|status|compare)")

        if subcommand == "migrate":
            self._migrate_all()
        elif subcommand == "sync":
            self._sync_all(
                apps_filter=options.get("apps"),
                models_filter=options.get("models"),
                exclude_filter=options.get("exclude"),
                safe=options.get("safe", True),
            )
        elif subcommand == "flush":
            self._flush_all(noinput=options["noinput"])
        elif subcommand == "dump":
            self._dump_all(options["output_dir"])
        elif subcommand == "load":
            self._load_all(options["input_dir"])
        elif subcommand == "status":
            self._status_all()
        elif subcommand == "compare":
            self._compare_all(
                repair=options["repair"],
                interactive=options["interactive"],
                dry_run=options["dry_run"],
            )
        else:
            raise CommandError(f"Unknown subcommand: {subcommand}")

    # ----------------------------
    # Subcommand implementations
    # ----------------------------

    def _migrate_all(self):
        for alias in settings.DATABASES.keys():
            self.stdout.write(self.style.WARNING(f"Applying migrations on {alias}..."))
            call_command("migrate", database=alias, interactive=False, verbosity=1)
            self.stdout.write(self.style.SUCCESS(f"✔ Done with {alias}"))

    def _sync_all(self, apps_filter=None, models_filter=None, exclude_filter=None, safe=True):
        from multidb_project.routers import MultiDBRouter
        router = MultiDBRouter()

        default_alias = "default"
        skip_apps = {"auth", "contenttypes", "sessions", "admin"}

        # Parse filters
        apps_filter = set(apps_filter.split(",")) if apps_filter else None
        models_filter = set(models_filter.split(",")) if models_filter else None
        exclude_filter = set(exclude_filter.split(",")) if exclude_filter else set()

        # Replication order: ensure dependencies first
        model_order = []
        customer_model = apps.get_model("customers", "Customer")
        order_model = apps.get_model("orders", "Order")
        if customer_model in apps.get_models():
            model_order.append(customer_model)
        for m in apps.get_models():
            if m not in model_order:
                model_order.append(m)

        for alias in settings.DATABASES.keys():
            if alias == default_alias:
                continue
            self.stdout.write(self.style.WARNING(f"Syncing data into {alias}..."))

            for model in model_order:
                app_label = model._meta.app_label
                model_name = f"{app_label}.{model.__name__}"

                if app_label in skip_apps:  # 🚫 Skip Django system apps
                    continue
                if app_label in exclude_filter or model_name in exclude_filter:  # 🚫 Skip excluded apps or models
                    self.stdout.write(self.style.NOTICE(f"Skipping {model_name}"))
                    continue
                if apps_filter and app_label not in apps_filter:  # ✅ Apply include filters
                    continue
                if models_filter and model_name not in models_filter:
                    continue

                objs = model.objects.using(default_alias).all() # Get all objects from default DB
                if not objs:
                    continue

                # Handle dependencies: ensure Customer exists before Order
                if model_name == "orders.Order":
                    customer_objs = {c.pk: c for c in customer_model.objects.using(default_alias).all()}

                for obj in objs:
                    row = {
                        f.attname if f.get_internal_type() == "ForeignKey" else f.name: getattr(obj, f.attname)
                        if f.get_internal_type() == "ForeignKey" else getattr(obj, f.name)
                        for f in model._meta.local_fields
                    }

                    if model_name == "orders.Order":  # Replicate Customer dependency for Orders
                        customer = customer_objs.get(row["customer_id"])
                        if customer:
                            try:
                                router.start_replication(alias)
                                customer_model.objects.using(alias).update_or_create(
                                    pk=customer.pk,
                                    defaults={"name": customer.name, "email": customer.email},
                                )
                            finally:
                                router.stop_replication(alias)

                    try:  # Replicate main object
                        router.start_replication(alias)
                        model.objects.using(alias).update_or_create(pk=row["id"], defaults=row)
                    finally:
                        router.stop_replication(alias)

                self.stdout.write(f"  Synced {model_name}: {objs.count()} rows")

            self.stdout.write(self.style.SUCCESS(f"✔ Finished syncing {alias}"))

    def _flush_all(self, noinput=False):
        for alias in settings.DATABASES.keys():
            self.stdout.write(self.style.WARNING(f"Flushing {alias}..."))
            call_command("flush", database=alias, interactive=not noinput, reset_sequences=True)
            self.stdout.write(self.style.SUCCESS(f"✔ Flushed {alias}"))

    def _dump_all(self, output_dir):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for alias in settings.DATABASES.keys():
            filename = os.path.join(output_dir, f"{alias}.json")
            self.stdout.write(self.style.WARNING(f"Dumping {alias} into {filename}..."))
            with open(filename, "w", encoding="utf-8") as f:
                call_command("dumpdata", database=alias, indent=2, stdout=f)
            self.stdout.write(self.style.SUCCESS(f"✔ Dumped {alias}"))

    def _load_all(self, input_dir):
        for alias in settings.DATABASES.keys():
            filename = os.path.join(input_dir, f"{alias}.json")
            if not os.path.exists(filename):
                self.stdout.write(self.style.ERROR(f"❌ No fixture for {alias} at {filename}"))
                continue
            self.stdout.write(self.style.WARNING(f"Loading {alias} from {filename}..."))
            call_command("loaddata", filename, database=alias)
            self.stdout.write(self.style.SUCCESS(f"✔ Loaded {alias}"))

    def _status_all(self):
        """Check table and row counts per DB"""
        for alias in settings.DATABASES.keys():
            self.stdout.write(self.style.MIGRATE_HEADING(f"\n📊 Status for {alias}"))
            connection = connections[alias]
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT count(*) FROM sqlite_master" if "sqlite" in connection.vendor else "SELECT count(*) FROM information_schema.tables WHERE table_schema = 'public'" if connection.vendor == "postgresql" else "SELECT count(*) FROM information_schema.tables WHERE table_schema = DATABASE()")
                    table_count = cursor.fetchone()[0]
            except Exception as e:
                self.stdout.write(self.style.ERROR(f"❌ Could not fetch table count: {e}"))
                table_count = 0

            self.stdout.write(self.style.WARNING(f"Tables: {table_count}"))

            for model in apps.get_models():
                try:
                    count = model.objects.using(alias).count()
                    self.stdout.write(f"  {model.__name__}: {count}")
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"  {model.__name__}: ❌ {e}"))

    def _compare_all(self, repair=False, interactive=False, dry_run=False):
        """Compare row counts across DBs, optionally repair mismatches"""
        from multidb_project.routers import MultiDBRouter
        router = MultiDBRouter()

        dbs = list(settings.DATABASES.keys())
        default_alias = "default"

        self.stdout.write(self.style.MIGRATE_HEADING("\n🔍 Comparing databases..."))

        for model in apps.get_models():
            counts = {}
            for alias in dbs:
                try:
                    counts[alias] = model.objects.using(alias).count()
                except Exception:
                    counts[alias] = None

            if len(set(counts.values())) == 1:
                self.stdout.write(self.style.SUCCESS(f"✔ {model.__name__} counts match: {counts}"))
            else:
                self.stdout.write(self.style.ERROR(f"❌ {model.__name__} mismatch: {counts}"))

                # Decide repair mode
                do_repair = repair
                if interactive and not repair:
                    answer = input(f"🔧 Repair {model.__name__}? (y/n): ").strip().lower()
                    do_repair = (answer == "y")

                if not do_repair:
                    continue

                if dry_run:
                    self.stdout.write(
                        self.style.WARNING(f"[Dry-run] Would repair {model.__name__} from {default_alias} into others")
                    )
                    continue

                try:
                    objs = model.objects.using(default_alias).all()

                    # Ensure dependencies first
                    if model._meta.model_name == "order":
                        customer_model = apps.get_model("customers", "Customer")
                        customer_objs = {c.pk: c for c in customer_model.objects.using(default_alias).all()}

                    for obj in objs:
                        data = {
                            f.name: getattr(obj, f.attname) if f.get_internal_type() == "ForeignKey" else getattr(obj, f.name)
                            for f in model._meta.fields if f.name != "id"
                        }

                        if model._meta.model_name == "order":
                            customer = customer_objs.get(data["customer_id"])
                            if customer:
                                try:
                                    router.start_replication(alias)
                                    customer_model.objects.using(alias).update_or_create(
                                        pk=customer.pk,
                                        defaults={"name": customer.name, "email": customer.email},
                                    )
                                finally:
                                    router.stop_replication(alias)

                        for alias in dbs:
                            if alias == default_alias:
                                continue
                            try:
                                router.start_replication(alias)
                                model.objects.using(alias).update_or_create(pk=obj.pk, defaults=data)
                            finally:
                                router.stop_replication(alias)

                    self.stdout.write(self.style.SUCCESS(f"✔ {model.__name__} repaired"))
                except Exception as e:
                    self.stdout.write(self.style.ERROR(f"❌ Failed to repair {model.__name__}: {e}"))
