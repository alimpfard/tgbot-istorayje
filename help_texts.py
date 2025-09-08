from telegram.constants import ParseMode


HELP_TEXTS = [
    {
        "list": [
            (
                "*Message Tagger/Indexer*\n"
                + "To use this bot for tagging and indexing messages, follow these steps:\n"
                + "    0. Create two Telegram channels (with usernames), hereon called _temp_ and _storage_\n"
                + "    1. Add the bot to both channels as an admin\n"
                + "    2. In this chat, run: /connect <name-of-collection>\n"
                + "    3. In this chat, run: /set _@storage_\n"
                + "    4. In this chat, run: /temp _@temp_\n"
                + 'To tag and add a message (or a list of messages) to a collection, send the message(s) to _storage_, then reply with "set: _tags_" in the channel.\n'
                + "\nExample interaction:"
            ),
            "This is a test message",
            "set: test msg message useless example",
            (
                "You can add multiple messages at once by selecting and forwarding them, then replying with the tag command.\n"
                "The tags you provide will be associated with the message(s) in the collection.\n"
                "You can repeat this process for any number of collections."
            ),
        ],
        "mode": ParseMode.MARKDOWN,
    },
    {
        "list": [
            (
                "To search any collection, use the bot's inline interface in any chat:\n"
                + "    @istorayjebot _collection_ _query_ _{caption}_\n"
                + "You can also create aliases for collections (including external ones) with:\n"
                + "    /alias set <alias> <value>\n"
                + "  Example: /alias set ac @anilist:char\n"
                + "To set an implicit alias (so that '@botname <query>' is always treated as '@botname <alias> <query>'), use:\n"
                + "    /alias implicit <alias>\n"
                + "  Example: /alias implicit @dict\n"
                + "The caption in curly braces is optional. To use the default caption, use any of: `$def`, `$default`, or `$`\n"
                + "Examples:\n"
                + "    @istorayjebot gif misaka nah {$}   # search 'gif' for 'misaka' and 'nah', use default caption\n"
                + "    @istorayjebot gif lol              # search 'gif' for 'lol', no caption\n"
                + "    @istorayjebot gif fish {Help, I am drowning!}  # search 'gif' for 'fish', custom caption\n"
                + "\nExternal pseudo-collections (collections starting with '@') are supported.\n"
                + "    `@anilist`: query anilist for anime/manga/character info\n"
                + "        Modifiers: `ql` (custom GraphQL), `bychar` (search anime by character), `char` (character search), `aggql` (aggregate GraphQL results)\n"
                + "    `@saucenao`: reverse image search using SauceNao\n"
                + "    `@iqdb`: reverse image search using IQDB\n"
                + "    `@google`: reverse image search using Google\n"
                + "    `@dan`: neural net image tagger (danbooru tags)\n"
                + "    `@trace`: anime trace search\n"
                + "    `@api:<name>`: custom API endpoints (see /api help)\n"
                + "\nYou can use these external sources in the same way as normal collections."
            ),
        ],
        "mode": ParseMode.MARKDOWN,
    },
    {
        "list": [
            (
                "To modify tags or entries, reply to a tagged message in the storage channel with one of the following commands:\n"
                + "    `^set:` - set the tags, overwrites all previous tags\n"
                + "    `^add:` - add tags to the existing set\n"
                + "    `^remove:` - remove tags from the existing set\n"
                + "    `^delete` - delete the entry from the index\n"
                + "\nExample:"
            ),
            "^add: newtag another-new-tag",
            (
                "You can chain these commands as needed.\n"
                "Use `^set:` to completely replace tags, or `^add:`/`^remove:` to incrementally update them.\n"
                "`^delete` will remove the message from the collection entirely."
            ),
        ],
        "mode": ParseMode.MARKDOWN,
    },
    {
        "list": [
            (
                "You can use special 'magic' tags in tag commands to trigger extra features. These are used in the same place as normal tags, and can automate or enhance tagging.\n"
                "Some available magic tags:\n"
                "    $google(_min accuracy_)   # reverse search Google for tags\n"
                "    $anime(_min accuracy_)    # try to find the anime title (works best on uncropped images)\n"
                "    $caption(_default caption_) # set a default caption for the message (media only)\n"
                "    $sauce(_min accuracy_)    # reverse search SauceNao for source\n"
                "    $dan(_min accuracy_)      # neural net tagger (danbooru tags)\n"
                "    $synonyms [options]       # add synonyms/related words (see magic help)\n"
                "    $gifop [options]          # perform operations on GIFs (reverse, speed, etc)\n"
                "\nThe default minimum accepted accuracy is 60. For $caption, commas in captions must be escaped with a backslash.\n"
                "See /help magics for a full listing and details of all magic tags and their arguments."
            ),
        ],
        "mode": ParseMode.MARKDOWN,
    },
    {
        "list": [
            (
                "Other features and commands:\n"
                + "\n- /rehash: Rebuild the index for all collections (admin only)\n"
                + "- /share: Share a collection with another user or chat\n"
                + "- /api: Manage and use custom API endpoints (see /api help for full documentation)\n"
                + "- /temp: Set the temp channel for staging messages\n"
                + "- /set: Set the storage channel for a collection\n"
                + "- /connect: Connect this chat to a collection\n"
                + "- /alias: Manage collection aliases (see above)\n"
                + "- /help: Show this help message\n"
                + "- /help magics: Show detailed documentation for all magic tags\n"
                + "- .ext <command>: Run an external command on a message (advanced, see below)\n"
                + "\nThe bot supports multiple collections, external sources, and advanced search and tagging features.\n"
                + "You can use inline queries, reply commands, and magic tags in combination.\n"
                + "For more details on any command, use /help or /help magics."
            ),
            (
                "About `.ext` commands:\n"
                + "  You can reply to a message in the storage channel with `.ext <command>` to trigger an external or custom operation on that message.\n"
                + "  The available external commands depend on your configuration and may include API calls, custom processing, or integrations.\n"
                + "  This is an advanced feature for users who want to extend the bot's functionality.\n"
                + "\nSpecial forms for `.ext` commands:\n"
                + "  - `.ext .schedule <interval> <command>`: Schedule the given command to run repeatedly every <interval> seconds (or a time string, e.g. '5m').\n"
                + "  - `.ext .unschedule <job_id>`: Cancel a previously scheduled job by its job ID.\n"
                + "  - `.ext .first <command>`: Only process the first result of the command.\n"
                + "  - `.ext @each <command>\\nitem1\\nitem2...`: Run the command for each item in the list, replacing (it) in the command with the item. Each item should be on its own line after the command.\n"
                + "\nExamples:\n"
                + "  `.ext .schedule 10m @api:myapi do something`\n"
                + "  `.ext .unschedule 66b2e...`\n"
                + "  `.ext .first @api:myapi query`\n"
                + "  `.ext @each @api:myapi do (it)\\nfoo\\nbar`\n"
            ),
        ],
        "mode": ParseMode.MARKDOWN,
    },
]

MAGIC_HELP_TEXTS = [
    "Magic Tags Index\n"
    "\n"
    "TODO: Add online documentation for this stuff\n"
    "This is a listing of all supported magic tags and their properties:\n",
    "`$google` - search google for relevant tags\n"
    "  stage 2\n"
    "  arguments:\n"
    "      - positional _minimum accepted accuracy_ <int>: tags with confidence less than this will be ignored\n"
    "      - optional literal _cloud_ <literal>: supposed to search with google Vision ML. currently ignored.\n"
    "  short forms:\n"
    "      None\n"
    "  document types:\n"
    "      media documents <image, video, GIF>\n"
    "  further notes:\n"
    "      None\n",
    "`$anime` - search for anime title\n"
    "  stage 2\n"
    "  arguments:\n"
    "       - position _minimum accepted accuracy_ <int>: results with confidence less than this will be ignored\n"
    "  short forms:\n"
    "      None\n"
    "  document types:\n"
    "      media documents <image, video, GIF>\n"
    "  further notes:\n"
    "      Cropped images of anime will likely yield incorrect results\n",
    "`$sauce` - search for image source (SauceNao)\n"
    "  stage 2\n"
    "  arguments:\n"
    "       - position _minimum accepted accuracy_ <int>: results with confidence less than this will be ignored\n"
    "  short forms:\n"
    "      None\n"
    "  document types:\n"
    "      media documents <image, video, GIF>\n"
    "  further notes:\n"
    "      May return multiple equal sources, and sources may have no links\n",
    "`$dan` - use a neural net to guess image contents (uses danbooru tags)\n"
    "  stage 2\n"
    "  arguments:\n"
    "       - position _minimum accepted accuracy_ <int>: results with confidence less than this will be ignored\n"
    "  short forms:\n"
    "      None\n"
    "  document types:\n"
    "      media documents <image, video, GIF>\n"
    "  further notes:\n"
    "      Geared towards animated/drawn images, but has shown to perform reasonably well on real images too\n",
    "`$synonyms` - find and add synonyms or related words\n"
    "  stage 1\n"
    "  arguments:\n"
    "      - mixed literal _word_ <literal> (multiple allowed): words to process\n"
    '      - optional opt mixed literal _:hypernym-depth {depth}_ <literal> (takes an int modifier "depth"): include words related by categories up to _depth_ categories\n'
    '      - optional opt mixed literal _:count {count}_ <literal> (takes an int modifier "count"): include this many results (default 10)\n'
    "      - optional opt literal _:hyponyms_ <literal>: includes words related in the same category\n"
    "  short forms:\n"
    "      `$syn`\n"
    "  document types:\n"
    "      all document types\n"
    "  further notes:\n"
    "      None\n",
    "`$caption` - add a default caption invokable by {$} in inline queries\n"
    "  stage 2\n"
    "  arguments:\n"
    '      - positional literal _caption_ <literal>: the would-be default caption (escape commas with a backslash "\\")\n'
    "  short forms:\n"
    "      `$cap`, `$defcap`\n"
    "  document types:\n"
    "      media documents <image, video, GIF>\n"
    "  further notes:\n"
    "      None\n",
    "`$gifop` - operations on indexed GIFs\n"
    "  stage 2\n"
    "  arguments:\n"
    "      - optional literal _reverse_ <literal>: reverses the GIF\n"
    "      - optional literal _append_ <literal>: (applies if _reverse_ is provided) appends the reverse to the end of original if provided\n"
    "      - optional literal _replace_ <literal>: replaces the original in the index if provided\n"
    "      - optional mixed literal _speed {speed}_ (takes a float modifier _speed_) <literal>: modifies the speed of the GIF\n"
    "      - optional mixed literal _skip {value} <ti|fr|%>_ (takes an int modifier _value_) <literal>: skips the provided {value} units from the start\n"
    "      - optional mixed literal _early {value} <ti|fr|%>_ (takes an int modifier _value_) <literal>: cuts off the provided {value} units from the end\n"
    "      - optional mixed literal _animate frame:[number]/[unit] length:[number]/[unit] effect:[name] dx:[number] dy:[number] (multiplex) (word:word)_\n"
    "           note: to escape spaces in e.g. 'text', use `^ ` (that is, caret-space)\n"
    "           effects:\n"
    "           + scroll - scroll in direction (dx, dy)\n"
    "           + zoom - zoom in to (dx, dy)\n"
    "           + rotate - rotate around (dx, dy)\n"
    "           + text - place (prop: `text`) at position (dx, dy)\n"
    "             extras: `[color:<fill color>] [outline:<stroke color>] [background:<background color>] [shadow:<shadow color>]`\n"
    "             not specifying `shadow` and `background` will disable them\n"
    "           + overlay-points - overlay key point positions\n"
    "           + distort - apply various distortions (TODO)\n"
    "  short forms:\n"
    "      None\n"
    "  document types:\n"
    "      GIFs\n"
    "  further notes:\n"
    "      operation order is (first to last):\n"
    "          skip|early, reverse, speed\n",
    """/api documentation

the available subcommands are

- list {api/input/output}
    List the required API handlers

- define {input/output} {name} {varname} ...request in API DSL
    Defines the named adapter with the provided varname,
    for input adapters, the variable refers to the query (as a string)
    for output adapters, the variable refers to the response, parsed if possible

- redefine (same args as define)
    Overwrites an existing adapter

- declare {name} {comm type} {input adapter} {output adapter} {api path}
    Declares an API endpoint, accessible via the @{name} external collection
    comm type will be defined later
    api path may refer to the result of {input adapter} as result

- redeclare (same args as declare)
    Overwrites a declared api

API handler documentation

Comm types

"metavars" are of the following forms:
- `$var` (as declared in the API), refers to some variable `var` as the result of a previous step (e.g. input adapter result in the output adapter, or user input in the input adapter)
- `#page[name](...#name...)` (e.g. `#page[p](&pid=#p)`), refers to the page value as passed in the query (prefix +pagenum: `+1 collection query`), resolves to the subexpression with the metavar replaced (`&pid=1` if `+1` is the current page) or nothing if no page is passed.

the available comm types are:
- http/link
    metavar in url: Yes
    response type: string
    behaviour: simply substitutes the metavar in the URL
    expected input adapter output: string

- json/post
    metavar in url: Yes, through input.pvalue
    response type: Dictionary
    behvaiour: substitutes the metavar, sends a post request, returns output
    expected input adapter output: {pvalue?: string, value?: T}

- html/xpath
    metavar in url: Yes
    response type: Dictionary
    behvaiour: substitutes the metavar, sends a get request, returns output
    expected input adapter output: string

- graphql
    metavar in url: No
    response type: T
    behaviour: sends the metavar as a gql query
    expected input adapter output: T'


DSL Documentation

A single python expression with the following extensions

- expr @json - jsonifies expr
- expr @image - if `expr` is a dict of {url, caption?, thumb_url}, yields that as an input media result, otherwise tries to download the url (data URI supported).
- expr @freshVar - yields an identifier that can be used to retrieve `expr` in a later query (no lifetime guarantee)
- expr @varStore - yields a value associated with `expr` as an identifier; see @freshVar
- expr @query - if `expr` refers to the input of the adapter, yields the original query string (otherwise nothing)
- expr @catch - catches exceptions in expr, yields None on exception
- expr @(nextStep(description)) - yields `expr`, with an inline keyboard attached according to `description`:
    description is either a "query" string, or a dictionary of {button text: query string}
    where the "query" string itself is either an https:// URL, or a further query to pass to the bot inline interface
    To refer to external collections, surround the collection with backticks (not monospaced, literal backtick characters)
- ${metavar} - replaced with the value of the metavar


Output Adapter Documentation

All output adapters must evaluate to an array of 2-tuples of the form (result name, result text), both of which should be strings.""",
]
