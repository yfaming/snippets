#!/usr/bin/env python
# -*- coding: utf-8 -*-
import httplib2
from argparse import Namespace
from bidict import bidict

from apiclient import discovery
from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage


def get_credentials(credential_path, client_secret_path, scopes):
    store = Storage(credential_path)
    credentials = store.get()

    # client_secret 和 credential 是不同的
    if not credentials or credentials.invalid:
        flow = client.flow_from_clientsecrets(client_secret_path, scopes)
        flags = Namespace(auth_host_name='localhost',
                          auth_host_port=[8080, 8090],
                          logging_level='ERROR',
                          noauth_local_webserver=True)
        credentials = tools.run_flow(flow, store, flags)
    return credentials


def get_service(credential_path, client_secret_path, scopes):
    credentials = get_credentials(credential_path, client_secret_path, scopes)
    http = credentials.authorize(httplib2.Http())
    discovery_url = 'https://sheets.googleapis.com/$discovery/rest?version=v4'
    service = discovery.build('sheets', 'v4', http=http, discoveryServiceUrl=discovery_url)
    return service


def next_column(column):
    def char_add_one_or_zero(c, n):
        assert n in (0, 1)
        if n == 0:
            return c, False
        else:
            if c == 'Z':
                return ('A', True)
            else:
                return (chr(ord(c) + 1), False)

    assert column
    res = []
    n = 1
    for c in reversed(column.upper()):
        char, n = char_add_one_or_zero(c, n)
        res.append(char)
    if n:
        res.append('A')
    return ''.join(reversed(res))


class SheetTable(object):
    """
    对应一个 sheet。
    以第一行的值，作为列名，且假定第一列不为空。
    """
    def __init__(self, service, spreadsheet_id, title, column_dict=None):
        self.service = service
        self.spreadsheet_id = spreadsheet_id
        self.title = title
        # bidict, column_name => column_id in A1 notation(A, B, C, ...)
        if column_dict is None:
            self.column_dict = self.reflect_columns()
        else:
            self.column_dict = column_dict

    def reflect_columns(self):
        """取第一行的值，作为列名"""
        range = u'{}!1:1'.format(self.title)
        res = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=range).execute()
        column_names = res['values'][0] if res.get('values', []) else []

        column_dict = bidict()
        column_id = 'A'
        for column_name in column_names:
            column_dict[column_name] = column_id
            column_id = next_column(column_id)
        return column_dict

    def insert_row(self, row, value_input_option='USER_ENTERED'):
        """
        row: column_name -> value
        value_input_option:
        * RAW: The values will not be parsed and will be stored as-is.
        * USER_ENTERED: The values will be parsed as if the user typed them into the UI.
        ref: https://developers.google.com/sheets/api/reference/rest/v4/ValueInputOption
        """
        for column_name in row:
            assert column_name in self.column_dict, 'column {} not found'.format(column_name)

        row_id = self.next_empty_row_id()
        data = []
        for column_name, value in row.items():
            column_id = self.column_dict[column_name]
            data.append({
                'range': '{}!{}{}:{}{}'.format(self.title, column_id, row_id, column_id, row_id),
                'values': [[value]],
            })

        body = {
            'valueInputOption': value_input_option,
            'data': data,
        }
        self.service.spreadsheets().values().batchUpdate(spreadsheetId=self.spreadsheet_id, body=body).execute()

    def next_empty_row_id(self):
        range = u'{}!A:A'.format(self.title)
        res = self.service.spreadsheets().values().get(spreadsheetId=self.spreadsheet_id, range=range).execute()
        return len(res.get('values', [])) + 1

    def clear_all_data(self):
        """删除全部数据（第一行除外）"""
        max_row_id = self.next_empty_row_id()
        column_id = 'A'
        for _ in self.column_dict:
            column_id = next_column(column_id)
        range = '{}!A2:{}{}'.format(self.title, column_id, max_row_id)
        self.service.spreadsheets().values().clear(spreadsheetId=self.spreadsheet_id, range=range, body={}).execute()


class SpreadSheetDB(object):
    """对应一个 spreadsheet"""
    def __init__(self, service, spreadsheet_id):
        self.service = service
        self.spreadsheet_id = spreadsheet_id
        self.sheets = {}

        self.reflect_meta()

    def reflect_meta(self):
        sheets = self.service.spreadsheets().get(spreadsheetId=self.spreadsheet_id).execute()['sheets']
        for sheet in sheets:
            properties = sheet['properties']
            title = properties['title']

            self.sheets[title] = SheetTable(service=self.service,
                                            spreadsheet_id=self.spreadsheet_id,
                                            title=title)

    def __getattr__(self, name):
        """支持用 attribute 方式访问 spreadsheet 里的 sheet"""
        if name in self.sheets:
            return self.sheets[name]
        else:
            raise AttributeError('attribute {} not found'.format(name))

    def create_sheet(self, title, column_names):
        assert title not in self.sheets, "sheet {} already exists".format(title)
        # 先创建 sheet
        body = {
            'requests': [
                {
                    'addSheet': {
                        'properties': {
                            'title': title,
                        }
                    }
                }
            ]
        }
        self.service.spreadsheets().batchUpdate(spreadsheetId=self.spreadsheet_id, body=body).execute()

        column_dict = bidict()
        column_id = 'A'
        for column_name in column_names:
            column_dict[column_name] = column_id
            column_id = next_column(column_id)
        sheet = SheetTable(self.service, self.spreadsheet_id, title, column_dict=column_dict)
        self.sheets[title] = sheet

        # 把列名写入第一行
        sheet.insert_row({column_name: column_name for column_name in column_names}, 'RAW')
        return sheet
