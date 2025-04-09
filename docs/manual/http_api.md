HTTP API
========

This section covers the HTTP API used to interact with the Near-Line Data
Store (NLDS), and the HTTP messages that must be sent to the API to carry out
tasks with the NLDS.  It is divided upon task type:

* [PUT / PUTLIST command](#put-command)
* [PUTLIST command](#putlist-command)
* [GET command](#get-command)
* [GETLIST command](#getlist-command)
* [DEL command](#del-command)
* [DELLIST command](#dellist-command)

### PUT / PUTLIST command

The `PUT` and `PUTLIST` commands have been combined into a single HTTP API function, as transferring
a single file is just the same as transferring a list of files - except there is only one filepath
in the list.  Therefore, the filepath(s) are specified in the body of the HTTP message, and the other
parameters, some of which are optional, are specified in the header / input parameter part of the message.
Any optional metadata parameters, such as the holding label, id or tags are also specified in the body.

| API endpoint | /files |
|---|---|
| HTTP method  | PUT |
| Parameters   | transaction_id: `UUID` |
|              | token: `str` (OAuth2 token) |
|              | user: `str` |
|              | group: `str` |
|              | tenancy: `str` (Optional) |
|              | job_label: `str` (Optional) |
|              | access_key: `str` |
|              | secret_key: `str` |
| Body         | *JSON*: `Dict {"filelist" : List<str>, "label": str, "tag": Dict<str, str>, "holding_id": int}` |
| Example      | `/files/put?transaction_id=1;user="bob";group="root"` |
| Body example | `{"filelist" : ["file1", "file2", "file3"]}`|


### GET command

The `GET` command is no longer used by the NLDS client, but it is left in the HTTP API in case
another application wishes to use a very simple GET function.  All of the parameters, some of which
are optional, are specified in the header / input parameters.  This method does not support the
full features of specifying a holding label, or id, or tags, as the `GET` HTTP method does not support
a body.

| API endpoint | /files |
|---|---|
| HTTP method  | GET |
| Parameters   | transaction_id: `UUID` |
|              | token: `str` (OAuth2 token) |
|              | user: `str` |
|              | group: `str` |
|              | filepath: `str` |
|              | target: `str` (Optional) |
|              | tenancy: `str` (Optional) |
|              | job_label: `str` (Optional) |
|              | access_key: `str` |
|              | secret_key: `str` |
| Body         | none |
| Example      | `GET /files/transaction_id=1;user="bob";group="root";filepath="myfile.txt"` |

### GETLIST command

The `GETLIST` command is used to get a list of files from the NLDS.  Of course, this list could have
only a single file in it, meaning that the functionality overlaps with the `GET` command somewhat.
The `GETLIST` uses the HTTP PUT method (rather than GET) so that a body can be supplied with the
HTTP message.  It supports the full features of specifying a label, id or tags.

| API endpoint | /files/getlist |
|---|---|
| HTTP method  | PUT |
| Parameters   | transaction_id: `UUID` |
|              | token: `str` (OAuth2 token) |
|              | user: `str` |
|              | group: `str` |
|              | tenancy: `str` (Optional) |
|              | target: `str` (Optional) |
|              | job_label: `str` (Optional) |
|              | access_key: `str` |
|              | secret_key: `str` |
| Body         | *JSON*  `Dict {"filelist" : List<str>, "label": str, "tag": Dict<str, str>, "holding_id": int}`|
| Example      | `/files/getlist?transaction_id=1;user="bob";group="root";`|
| Body example | `{"filelist" : ["file1", "file2", "file3"]}`|

<!-- ### DEL command

| API endpoint | /files |
|---|---|
| HTTP method  | DELETE |
| Parameters   | transaction_id |
|              | user |
|              | group |
|              | filepath |
|              | tenancy |
|              | access_key |
|              | secret_key |
| Body         | none |
| Example      | `/files/transaction_id=1;user="bob";group="root";filepath="myfile.txt" `| -->

### DEL / DELLIST command

Like the `PUT` / `PUTLIST` command, the `DEL` / `DELLIST` commands are combined into a single HTTP 
PUT method.  This is because deleting a single file is the same as deleting a list of files, just
with a single file.  Like the `PUT` / `PUTLIST` command, the filelist is specified in the body,
along with the metadata commands, with the input parameters / header passing the transaction id,
user, group, etc.

| API endpoint | /files/dellist |
|---|---|
| HTTP method  | PUT |
| Parameters   | transaction_id: `UUID` |
|              | token: `str` (OAuth2 token) |
|              | user: `str` |
|              | group: `str` |
|              | tenancy: `str` (Optional) |
|              | job_label: `str` (Optional) |
|              | access_key: `str` |
|              | secret_key: `str` |
| Body         | *JSON*  `Dict {"filelist" : List<str>, "label": str, "tag": Dict<str, str>, "holding_id": int}`|
| Example      | `/files/dellist?transaction_id=1;user="bob";group="root"`|
| Body example | `{"filelist" : ["file1", "file2", "file3"]}`|