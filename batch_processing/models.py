import logging

from django.conf import settings
from django.db import models
from django.db.models import fields

from django.utils.translation import gettext_lazy as _


class Json_File_Doc(models.Model):
    json_doc = models.FileField(upload_to='json_doc_upload/')
    # We could track other data -- a timestamp, perhaps. For now, no

class Batch_Object_Data_Item(models.Model):
    # Schema gives no limit on key or value size.  Using 128 as it seems adequate without further requirements
    key = models.CharField(
        unique=False,
        max_length=128,
        null=False,
        blank=False,
        db_index=True,
        help_text=_(
            "Key for a key/value pair"
        ),
        verbose_name=_("Key"),
    )
    # Indexing Key AND Value.  There are search requirements on both
    value = models.CharField(
        unique=False,
        max_length=128,
        null=False,
        blank=False,
        db_index=True,
        help_text=_(
            "Value for a key/value pair"
        ),
        verbose_name=_("Value"),
    )
    object = models.ForeignKey(
        "batch_processing.Batch_Object",
        null=False,
        blank=False,
        on_delete=models.CASCADE,
        help_text=_("The object this data item is associated with."),
    )

class Batch_Object(models.Model):
    # Schema gives no limit on object ID size.  Using 128 as it seems adequate without further requirements
    # While object_id looks like it should be unique, there is no such constraint mentioned in the requirements
    # But we will index on it.
    object_identifier = models.CharField(
        unique=False,
        max_length=128,
        null=False,
        blank=False,
        db_index=True,
        help_text=_(
            "Object identifier"
        ),
        verbose_name=_("Object ID"),
    )
    batch = models.ForeignKey(
        "batch_processing.Batch",
        null=False,
        blank=False,
        on_delete=models.CASCADE,
        help_text=_("The batch this object is associated with."),
    )
    # We could speed up object retrieval at the expense of DB space by storing the raw Object JSON
    # here.

class Batch(models.Model):
    # Schema gives no limit on object ID size.  Using 128 as it seems adequate without further requirements
    # While batch_id looks like it should be unique, there is no such constraint mentioned in the requirements
    # But we will index on it.
    batch_identifier = models.CharField(
        unique=False,
        max_length=128,
        null=False,
        blank=False,
        db_index=True,
        help_text=_(
            "Batch identifier"
        ),
        verbose_name=_("Batch ID"),
    )
