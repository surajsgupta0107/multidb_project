# customers/signals.py
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from django.conf import settings
from .models import Customer
from multidb_project.routers import MultiDBRouter

router = MultiDBRouter()


@receiver(post_save, sender=Customer)
def replicate_customer(sender, instance, created, **kwargs):
    for alias in settings.DATABASES.keys():
        if alias == instance._state.db or router.is_replicating(alias):
            continue
        try:
            router.start_replication(alias)
            sender.objects.using(alias).update_or_create(
                pk=instance.pk,
                defaults={"name": instance.name, "email": instance.email},
            )
        finally:
            router.stop_replication(alias)


@receiver(post_delete, sender=Customer)
def delete_customer(sender, instance, **kwargs):
    for alias in settings.DATABASES.keys():
        if alias == instance._state.db or router.is_replicating(alias):
            continue
        try:
            router.start_replication(alias)
            sender.objects.using(alias).filter(pk=instance.pk).delete()
        finally:
            router.stop_replication(alias)
