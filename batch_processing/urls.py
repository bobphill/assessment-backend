"""URL dispatch for batch processing module


"""
from django.contrib import admin
from django.urls import path, re_path
from batch_processing.views import Upload_Batch_File, Upload_Batch_Body, RetrieveObject

urlpatterns = [
    path('file/', Upload_Batch_File.as_view(), name="file"),
    path('body/', Upload_Batch_Body.as_view(), name="body"),
    re_path(r'^object/(?P<object_id>[a-zA-Z0-9]*)/$', RetrieveObject.as_view(), name="object"),
]
