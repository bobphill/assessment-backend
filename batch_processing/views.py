"""
Batch processing module
"""


import logging
import os
from django.conf import settings
from django.shortcuts import render

from rest_framework.views import APIView
from rest_framework.response import Response
import rest_framework.status as status
from rest_framework.exceptions import ParseError
from rest_framework.parsers import JSONParser

# In case we decide to ever do a translation, it is easy enough to mark all our strings now.
# It is a pain to do it later
from django.utils.translation import gettext_lazy as _

from assessment.settings import BASE_DIR
import assessment.settings
from batch_processing.forms import Json_Doc_Upload_Form
from batch_processing.models import Batch_Object, Batch_Object_Data_Item, Batch
import json
import jsonschema
from rest_framework.negotiation import BaseContentNegotiation

class InternalServerError(Exception):
    """
    Raise this when we want a 500 error
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

class ClientRequestError(Exception):
    """
    Raise this when we want a 400 error
    """
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)

logger = logging.getLogger(__name__)


class JSONSchemaParser(JSONParser):
    """
    Parser helper class
    JSON schema validation approach from https://richardtier.com/2014/03/24/json-schema-validation-with-django-rest-framework/
    """
    def parse(self, stream, schema, media_type=None, parser_context=None):
        data = super(JSONSchemaParser, self).parse(stream, media_type,
                                                   parser_context)

        schema_dict = None
        try:
            schema_dict = json.loads(
                open(os.path.join(BASE_DIR, 'files', 'schema.json')).read()

            )
            logger.debug(f'Got schema dictionary {schema_dict}')
        except Exception as e:
            logger.debug(f'Exception obtaining JSON schema: {e}') # Should result in a 500 error
            raise InternalServerError(e)

        try:
            #jsonschema.validate(data, schema_dict)
            pass
        except ValueError as error:
            raise ParseError(detail=_('JSON does not conform to schema.'))
        else:
            return data

class IgnoreClientContentNegotiation(BaseContentNegotiation):
    def select_parser(self, request, parsers):
        """
        Select our JSONSchemaParser.
        """
        #return JSONSchemaParser
        return parsers[0]

    def select_renderer(self, request, renderers, format_suffix):
        """
        Select the first renderer in the `.renderer_classes` list.
        """
        return (renderers[0], renderers[0].media_type)


class Upload_Batch_File(APIView):
    """
    The view class for our batch processing method.

    """

    def __init__(self, **kwargs):
        """
        Standard init method.  Putting this here in case we want to do anything later,
        :param self:
        :param kwargs:
        :return:
        """
        super(Upload_Batch_File, self).__init__(**kwargs)
        # Perform other initialization here


    def get(self, request):
        form = Json_Doc_Upload_Form()
        return render(request, 'upload.djhtml', {
            'form': form
        })

    def post(self, request):
        """
        Basic POST method for REST API processing of incoming JSON batch data
        :param self:
        :param request:
        :return:

        Incoming data looks something like this:
            {
                "batch_id": "71a8a97591894dda9ea1a372c89b7987",
                "objects": [
                    {
                        "object_id": "d6f983a8905e48f29ad480d3f5969b52",
                        "data": [
                            {
                                "key": "type",
                                "value": "shoe"
                            },
                            {
                                "key": "color",
                                "value": "purple"
                            }, ...
                        ]
                    }, ...
                ]
            }

        """
        ## Accept data as either a POST body, or as a file
        batch_data = None
        file_obj = None
        try:
            # This part saves the uploaded file in its entirety.
            # We may not want to do this, for space reasons, etc.
            # We may also want to save it temporarily and then delete it
            # on successful completion of our tasks (keeping files triggering failures).
            # Certainly, for debugging purposes, there are advantages to retention
            form = Json_Doc_Upload_Form(request.POST, request.FILES)
            if form.is_valid():
                form.save()
            else:
                logger.error('Got no file from form')
                # We don't have anything good. Complain to the caller.
                return Response(
                    _(
                        'No data found in request. No file data in request.'
                    ),
                    status.HTTP_400_BAD_REQUEST
                )

            #file_obj = request.FILES.get("file", None)
            if file_obj:
                # Note -- this assumes that we are guaranteed to be able to read the file data into
                # memory without issue.  That MAY not be the case, in which case, more robust file
                # reading (in increments, with size checks) behooves us.
                batch_data = file_obj.read()
                logger.debug(f'Data from request file is {batch_data}')
        except TypeError:
            # no file object uploaded.  Eat the exception and see if we have a body argument
            logger.error('TypeError attempting to access file data')
            return Response(
                _(
                    'No data found in request. No file data in request.'
                ),
                status.HTTP_400_BAD_REQUEST
            )
        except Exception as e:
            logger.error(f'Exception attempting to access file data: {e}')
            return Response(
                _(
                    'No data found in request. No file data in request.'
                ),
                status.HTTP_400_BAD_REQUEST
            )

        # DO STUFF
        return Response(status.HTTP_200_OK)


class Upload_Batch_Body(APIView):
    """
    The view class for our batch processing method.

    """

    #parser_classes = (JSONSchemaParser)
    #content_negotiation_class = IgnoreClientContentNegotiation

    def __init__(self, **kwargs):
        """
        Standard init method.  Putting this here in case we want to do anything later,
        :param self:
        :param kwargs:
        :return:
        """
        super(Upload_Batch_Body, self).__init__(**kwargs)
        # Perform other initialization here

    def post(self, request):
        """
        Basic POST method for REST API processing of incoming JSON batch data
        :param self:
        :param request:
        :return:

        Incoming data looks something like this:
            {
                "batch_id": "71a8a97591894dda9ea1a372c89b7987",
                "objects": [
                    {
                        "object_id": "d6f983a8905e48f29ad480d3f5969b52",
                        "data": [
                            {
                                "key": "type",
                                "value": "shoe"
                            },
                            {
                                "key": "color",
                                "value": "purple"
                            }, ...
                        ]
                    }, ...
                ]
            }

        """
        ## Accept data as either a POST body, or as a file
        batch_data = None
        batch_dict = None
        try:
            # parser_classes called implicitly, according to
            # https://richardtier.com/2014/03/24/json-schema-validation-with-django-rest-framework/
            batch_data = request.data
            logger.debug(f'Data from request body is {batch_data}')
            logging.debug(f'Data from request body is {batch_data}')
        except ParseError as error:
            return Response(
                _(f'Request data cannot be parsed as JSON. Invalid JSON - {error.message}'),
                status.HTTP_400_BAD_REQUEST,
            )

        if not batch_data:
           # if we have no batch data at this point, we have a problem.  The request is malformed.
            return Response(
                _(
                    'No data found in request. No file or body data in request.'
                ),
                status.HTTP_400_BAD_REQUEST,
            )
        logger.debug('Got through to the end...')
        # DO STUFF
        try:
            #batch_dict = json.loads(batch_data)
            batch_dict = batch_data
        except Exception as e:
            logger.error(f'Exception loading JSON data: {e}')
            return Response(
                _(
                    'Request data cannot be parsed as JSON. Invalid request.'
                ),
                status.HTTP_400_BAD_REQUEST
            )
	# We have a dictionary. It should conform to schema.  Populate objects
        try:
            batch = Batch(batch_identifier = batch_dict['batch_id'])
            batch.save()
            batch_objects = []
            for element in batch_dict['objects']:
                batch_object = Batch_Object(object_identifier = element['object_id'], batch = batch)
                batch_object.save()
                batch_objects.append(batch_object)
                data_objects = []
                for item in element['data']:
                    data_object = Batch_Object_Data_Item(key = item['key'], value = item['value'], object = batch_object)
                    data_object.save()
                    data_objects.append(data_object)
            return Response(status.HTTP_200_OK)
        except Exception as e:
            logger.error(f'Unexpected problem assembling JSON return: {e}')
            return Response(
                _("The server failed while processing the request."),
                status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class RetrieveObject(APIView):
    """
    Retrieves an object by object ID
    """

    def get(self, request, object_id=None):

        if object_id is None or object_id == '':
            return Response(
                _("The required object id was not provided."),
                status.HTTP_400_BAD_REQUEST
            )
        logger.debug(f'Got object id {object_id}')
        batch_object = None
        try:
            batch_object = Batch_Object.objects.get(object_identifier__exact=object_id)
            logger.debug(
                f"Got object for Batch_Object {object_id}"
            )
            # Get other items???
        except Batch_Object.DoesNotExist:
            logger.error(f'Failed to find object with ID {object_id}')
            return Response(
                _("The request object was not found in the database."),
                status.HTTP_404_NOT_FOUND
            )

        # Carry on
        batch_object_data_items = None
        try:
            batch_object_data_items = Batch_Object_Data_Item.objects.filter(object_id=batch_object.id)
        except Batch_Object_Data_Item.DoesNotExist:
            logger.error(f'Failed to find data for object with ID {object_id}')
            return Response(
                _("The request object data was not found in the database."),
                status.HTTP_404_NOT_FOUND
            )
        
        # And end
        try:
            batch_object_dict = {}
            batch_object_dict['object_id'] =  batch_object.object_identifier
            batch_object_dict['objects'] = []
            for batch_object_data_item in batch_object_data_items:
                dict_item = {}
                dict_item['key'] = batch_object_data_item.key
                dict_item['value'] = batch_object_data_item.value
                batch_object_dict['objects'].append(dict_item)
            return Response(batch_object_dict,status.HTTP_200_OK )
        except Exception as e:
            logger.error(f'Unexpected problem assembling JSON return: {e}')
            return Response(
                _("The server failed while processing the request."),
                status.HTTP_500_INTERNAL_SERVER_ERROR
            )
