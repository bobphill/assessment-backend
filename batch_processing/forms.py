from django import forms
from batch_processing.models import Json_File_Doc

class Json_Doc_Upload_Form(forms.ModelForm):
    class Meta:
        model = Json_File_Doc
        fields = ('json_doc', )
