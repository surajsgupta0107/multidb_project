from django.db import models


class Customer(models.Model):
    name = models.CharField(max_length=100)
    email = models.EmailField(unique=True)

    def __str__(self):
        # return f"Customer {self.id} - {self.name} - {self.email}"
        return f"Customer {self.id} - {self.name}"
