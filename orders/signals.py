# orders/signals.py
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from django.conf import settings
from .models import Order
from multidb_project.routers import MultiDBRouter
from customers.models import Customer

router = MultiDBRouter()


def replicate_customer_if_needed(customer_instance, alias):
    """
    Ensure the customer exists in the target DB before replicating the order.
    """
    if not Customer.objects.using(alias).filter(pk=customer_instance.pk).exists():
        try:
            router.start_replication(alias)
            Customer.objects.using(alias).update_or_create(
                pk=customer_instance.pk,
                defaults={
                    "name": customer_instance.name,
                    "email": customer_instance.email,
                },
            )
        finally:
            router.stop_replication(alias)


@receiver(post_save, sender=Order)
def replicate_order(sender, instance, created, **kwargs):
    for alias in settings.DATABASES.keys():
        if alias == instance._state.db or router.is_replicating(alias):
            continue
        try:
            router.start_replication(alias)
            # Ensure customer exists first
            replicate_customer_if_needed(instance.customer, alias)

            # Replicate the order
            sender.objects.using(alias).update_or_create(
                pk=instance.pk,
                defaults={
                    "customer_id": instance.customer.pk,
                    "product": instance.product,
                    "amount": instance.amount,
                },
            )
        finally:
            router.stop_replication(alias)


@receiver(post_delete, sender=Order)
def delete_order(sender, instance, **kwargs):
    for alias in settings.DATABASES.keys():
        if alias == instance._state.db or router.is_replicating(alias):
            continue
        try:
            router.start_replication(alias)
            sender.objects.using(alias).filter(pk=instance.pk).delete()
        finally:
            router.stop_replication(alias)
