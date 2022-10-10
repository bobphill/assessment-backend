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


def validate_json_against_schema(json_data):
    """
        JSON schema validation approach from https://richardtier.com/2014/03/24/json-schema-validation-with-django-rest-framework/

    :param json_data:
    :return:
    """
    schema_dict = None
    try:
        schema_dict = json.loads(
            open(os.path.join(BASE_DIR, 'files', 'schema.json')).read()

        )
        logger.debug(f'Got schema dictionary {schema_dict}')
    except Exception as e:
        logger.debug(f'Exception obtaining JSON schema: {e}')  # Should result in a 500 error
        raise InternalServerError(e)
    try:
        jsonschema.validate(json_data, schema_dict)
    except ValueError as error:
        raise ClientRequestError(_('JSON does not conform to schema.'))

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
        batch_dict = None
        file_obj = None
        try:
            # This part saves the uploaded file in its entirety.
            # We may not want to do this, for space reasons, etc.
            # We may also want to save it temporarily and then delete it
            # on successful completion of our tasks (keeping files triggering failures).
            # Certainly, for debugging purposes, there are advantages to retention
            form = Json_Doc_Upload_Form(request.POST, request.FILES)
            if not form.is_valid():
                logger.error('Got no file from form')
                # We don't have anything good. Complain to the caller.
                return Response(
                    _(
                        'No data found in request. No file data in request.'
                    ),
                    status.HTTP_400_BAD_REQUEST
                )

            file_obj = request.FILES.get("json_doc", None)
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
        try:
            batch_dict = json.loads(batch_data)
            validate_json_against_schema(batch_dict)
        except ClientRequestError:
            return Response(
                _(
                    'JSON data does not conform to schema.'
                ),
                status.HTTP_400_BAD_REQUEST
            )
        except InternalServerError:
            return Response(
                _(
                    'Unexpected problem validating JSON in request.'
                ),
                status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        except Exception as e:
            logger.error(f'Unexpected error validating JSON: {e}')
            return Response(
                _(
                    'Unexpected problem processing request.'
                ),
                status.HTTP_500_INTERNAL_SERVER_ERROR
            )


        # DO STUFF
        # We have a dictionary. It should conform to schema.  Populate objects
        try:
            batch = Batch(batch_identifier=batch_dict['batch_id'])
            logger.debug(f'Created batch {batch}')
            batch.save()
            batch_objects = []
            for element in batch_dict['objects']:
                batch_object = Batch_Object(object_identifier=element['object_id'], batch=batch)
                logger.debug(f'Created batch_object {batch_object}')
                batch_object.save()
                batch_objects.append(batch_object)
                data_objects = []
                for item in element['data']:
                    data_object = Batch_Object_Data_Item(key=item['key'], value=item['value'],
                                                         object=batch_object)
                    logger.debug(f'Created data_object {data_object}')
                    data_object.save()
                    data_objects.append(data_object)
            return Response(status.HTTP_200_OK)
        except Exception as e:
            logger.error(f'Unexpected problem assembling JSON return: {e}')
            return Response(
                _("The server failed while processing the request."),
                status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class Upload_Batch_Body(APIView):
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
            batch_dict = batch_data # Data already comes in dictionary format.  Not so with other approaches
        except Exception as e:
            logger.error(f'Exception loading JSON data: {e}')
            return Response(
                _(
                    'Request data cannot be parsed as JSON. Invalid request.'
                ),
                status.HTTP_400_BAD_REQUEST
            )
        try:
            validate_json_against_schema(batch_dict)
        except ClientRequestError:
            return Response(
                _(
                    'JSON data does not conform to schema.'
                ),
                status.HTTP_400_BAD_REQUEST
            )
        except InternalServerError:
            return Response(
                _(
                    'Unexpected problem validating JSON in request.'
                ),
                status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        except Exception as e:
            logger.error(f'Unexpected error validating JSON: {e}')
            return Response(
                _(
                    'Unexpected problem processing request.'
                ),
                status.HTTP_500_INTERNAL_SERVER_ERROR
            )
	    # We have a dictionary. It should conform to schema.  Populate objects
        try:
            batch = Batch(batch_identifier = batch_dict['batch_id'])
            logger.debug(f'Created batch {batch}')
            batch.save()
            batch_objects = []
            for element in batch_dict['objects']:
                batch_object = Batch_Object(object_identifier = element['object_id'], batch = batch)
                logger.debug(f'Created batch_object {batch_object}')
                batch_object.save()
                batch_objects.append(batch_object)
                data_objects = []
                for item in element['data']:
                    data_object = Batch_Object_Data_Item(key = item['key'], value = item['value'], object = batch_object)
                    logger.debug(f'Created data_object {data_object}')
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
            logger.warning(f'Possibly benign: Failed to find data for object with ID {object_id}')


        # And end
        try:
            batch_object_dict = {}
            batch_object_dict['object_id'] =  batch_object.object_identifier
            batch_object_dict['data'] = []
            for batch_object_data_item in batch_object_data_items:
                dict_item = {}
                dict_item['key'] = batch_object_data_item.key
                dict_item['value'] = batch_object_data_item.value
                batch_object_dict['data'].append(dict_item)
            return Response(batch_object_dict,status.HTTP_200_OK )
        except Exception as e:
            logger.error(f'Unexpected problem assembling JSON return: {e}')
            return Response(
                _("The server failed while processing the request."),
                status.HTTP_500_INTERNAL_SERVER_ERROR
            )


class RetrieveObjectArray(APIView):
    """
    Retrieves an array of objects by key or value.
    The easiest way is to use query parameters on a GET method, but that is a problematic approach
    in that the query parameters are exposed as part of the URL. Every Tom, Dick, and Harry sniffing
    packets knows what you are looking for.  If that isn't a problem, query parameters on a GET method
    are an easy approach.  BUT, if you want protection, you need to rely on the HTTPS encryption of the body,
    and use a POST (or a PUT -- arguments can be made either way) method with the value and/or key in the body
    And for the sake of implementation, I'm just going to support a single key and/or value.  Yeah, it's lazy, but
    if I don't impose that limit, I need to add logic for AND and OR operations, and that probably exceeds the scope
    of this little exercise.

    Also, there is no convenient way to search for null values.
    """

    def get(self, request, object_id=None):

        key = request.GET.get("key", None)
        value = request.GET.get("value", None)
        logger.debug(f'Got key {key} and value {value}')

        data_objects = None
        try:
            # Yes, for a more complex example, I'd use the Query language and just pass Query Expressions
            if (key and value):
                data_objects = Batch_Object_Data_Item.objects.filter(key=key, value=value)
            elif (key):
                data_objects = Batch_Object_Data_Item.objects.filter(key=key)
            elif (value):
                data_objects = Batch_Object_Data_Item.objects.filter(value=value)
            else:
                data_objects = Batch_Object_Data_Item.objects.all()

        except Batch_Object_Data_Item.DoesNotExist:
            logger.error(f'Failed to find object with ID {object_id}')
            return Response(
                _("The request object was not found in the database."),
                status.HTTP_404_NOT_FOUND
            )
        batch_objects=set()
        try:
            for data_object in data_objects:
                batch_objects.add(data_object.object)
        except Exception as e:
            logger.error(f'Unexpected exception gathering object IDs: {e}')
            return Response(
                _("Problem retrieving related information from database."),
                status.HTTP_500_INTERNAL_SERVER_ERROR
            )
        # batch_objects = None
        # try:
        #     batch_objects = Batch_Object.objects.filter(object__in=objects)
        # except Exception as e:
        #     logger.error(f'Unexpected exception gathering objects: {e}')
        #     return Response(
        #         _("Problem retrieving related information from database."),
        #         status.HTTP_500_INTERNAL_SERVER_ERROR
        #     )

        # And end
        batch_object_array = []
        for batch_object in batch_objects:
            logger.debug(f'Checking object {batch_object.object_identifier}')
            try:
                batch_object_dict = {}
                batch_object_dict['object_id'] = batch_object.object_identifier
                batch_object_dict['data'] = []
                batch_object_data_items = None
                try:
                    batch_object_data_items = Batch_Object_Data_Item.objects.filter(
                        object=batch_object)
                except Batch_Object_Data_Item.DoesNotExist:
                    logger.warning(
                        f'Possibly benign: Failed to find data for object with ID {batch_object.id}')
                for batch_object_data_item in batch_object_data_items:
                    dict_item = {}
                    dict_item['key'] = batch_object_data_item.key
                    dict_item['value'] = batch_object_data_item.value
                    batch_object_dict['data'].append(dict_item)
                batch_object_array.append(batch_object_dict)
            except Exception as e:
                logger.error(f'Unexpected problem assembling JSON return: {e}')
                return Response(
                    _("The server failed while processing the request."),
                    status.HTTP_500_INTERNAL_SERVER_ERROR
                )
        return Response(batch_object_array, status.HTTP_200_OK)

