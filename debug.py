from flask import Blueprint, request, jsonify, make_response
import os
import json
import re
import traceback
import requests as http_requests
import urllib.parse
from uuid import uuid4
from bson.json_util import dumps

debug_bp = Blueprint("debug", __name__)

# Will be set by register_debug(bot)
_bot = None


def register_debug(bot):
    global _bot
    _bot = bot


def _check_auth():
    pw = os.environ.get("DEBUG_PASSWORD")
    if not pw:
        return True  # no password configured, always full access
    given = request.args.get("pw") or request.headers.get("X-Debug-Password") or ""
    return given == pw


def _safe_serialize(obj):
    try:
        return json.loads(dumps(obj))
    except:
        return str(obj)


def _serialize_inline_result(result):
    """Serialize any InlineQueryResult* into a readable dict."""
    if result is None:
        return None
    from telegram import (
        InlineQueryResultArticle,
        InlineQueryResultCachedPhoto,
        InlineQueryResultCachedGif,
        InlineQueryResultCachedMpeg4Gif,
        InlineQueryResultCachedSticker,
        InlineQueryResultCachedDocument,
        InlineQueryResultCachedVoice,
        InlineQueryResultCachedAudio,
        InlineQueryResultPhoto,
        InlineQueryResultGif,
    )
    d = {"_type": type(result).__name__}
    # common fields
    for attr in ("id", "title", "caption", "description"):
        v = getattr(result, attr, None)
        if v is not None:
            d[attr] = str(v)
    # input_message_content
    imc = getattr(result, "input_message_content", None)
    if imc is not None:
        d["input_message_content"] = {
            "message_text": getattr(imc, "message_text", None),
            "parse_mode": getattr(imc, "parse_mode", None),
        }
    # file ids
    for attr in ("photo_file_id", "gif_file_id", "mpeg4_file_id",
                 "sticker_file_id", "document_file_id", "voice_file_id",
                 "audio_file_id"):
        v = getattr(result, attr, None)
        if v is not None:
            d[attr] = str(v)
    # urls (for non-cached results like InlineQueryResultPhoto)
    for attr in ("photo_url", "thumbnail_url", "gif_url", "mpeg4_url"):
        v = getattr(result, attr, None)
        if v is not None:
            d[attr] = str(v)
    # reply markup
    rm = getattr(result, "reply_markup", None)
    if rm is not None:
        try:
            buttons = []
            for row in rm.inline_keyboard:
                for btn in row:
                    b = {}
                    if btn.text:
                        b["text"] = btn.text
                    if btn.url:
                        b["url"] = btn.url
                    if btn.switch_inline_query_current_chat is not None:
                        b["switch_inline_query_current_chat"] = btn.switch_inline_query_current_chat
                    buttons.append(b)
            d["reply_markup"] = buttons
        except:
            d["reply_markup"] = str(rm)
    return d


def _serialize_results(results):
    """Serialize a list of InlineQueryResult objects."""
    return [_serialize_inline_result(r) for r in results if r is not None]


@debug_bp.route("/debug")
def debug_page():
    return make_response(DEBUG_HTML)


@debug_bp.route("/debug/parse", methods=["POST"])
def debug_parse():
    """Parse a query string and return the parsed components."""
    data = request.get_json(force=True)
    query_str = data.get("query", "")
    user_id = data.get("user_id")
    authed = _check_auth()

    original_query, coll, qstack, extra = _bot.parse_query(query_str)

    result = {
        "original_query": original_query,
        "collection": coll,
        "query_terms": qstack,
        "extra": extra,
    }

    if authed and user_id:
        try:
            user_id = int(user_id)
        except ValueError:
            pass
        resolved = _bot.resolve_alias(coll, user_id)
        result["resolved_collection"] = resolved

        for app in _bot.apps:
            try:
                implicit = _bot.resolve_alias(
                    f"implicit${app.bot.username}", user_id
                )
                if not implicit.startswith("implicit$"):
                    result.setdefault("implicit_collections", {})[
                        app.bot.username
                    ] = implicit
            except:
                pass

    return jsonify(result)


@debug_bp.route("/debug/stem", methods=["POST"])
def debug_stem():
    """Show how query terms get expanded via stemming."""
    data = request.get_json(force=True)
    query_str = data.get("query", "")
    authed = _check_auth()

    _, coll, qstack, extra = _bot.parse_query(query_str)

    result = {
        "original_terms": qstack,
    }

    if authed:
        expanded = _bot.process_search_query_further(qstack) if qstack else []
        result["expanded_terms"] = expanded
        stems = {}
        for terms in qstack:
            for term in terms:
                stems[term] = _bot.stemmer.stem(term)
        result["individual_stems"] = stems
    else:
        result["note"] = "Provide password for expanded stemming details"

    return jsonify(result)


@debug_bp.route("/debug/search", methods=["POST"])
def debug_search():
    """Run the full search pipeline and return intermediate data at each stage."""
    if not _check_auth():
        return jsonify({"error": "Full search requires authentication"}), 403

    data = request.get_json(force=True)
    query_str = data.get("query", "")
    user_id = data.get("user_id")

    if not user_id:
        return jsonify({"error": "user_id is required"}), 400
    try:
        user_id = int(user_id)
    except ValueError:
        return jsonify({"error": "user_id must be an integer"}), 400

    stages = {}

    # Stage 1: Parse
    original_query, coll, qstack, extra = _bot.parse_query(query_str)
    stages["1_parse"] = {
        "original_query": original_query,
        "collection": coll,
        "query_terms": qstack,
        "extra": extra,
    }

    # Stage 2: Alias resolution
    resolved_coll = _bot.resolve_alias(coll, user_id)
    stages["2_alias"] = {
        "input": coll,
        "resolved": resolved_coll,
        "changed": coll != resolved_coll,
    }
    coll = resolved_coll

    # External source?
    if coll.startswith("@"):
        remaining_query = " ".join(original_query.strip().split(" ")[1:])
        stages["3_routing"] = {
            "type": "external_source",
            "source": coll[1:],
            "remaining_query": remaining_query,
        }
        _run_external_source_debug(
            stages, coll[1:], remaining_query, extra, user_id
        )
        return jsonify(stages)

    # Validation
    if not coll or any(x in coll for x in "$./[]"):
        stages["3_routing"] = {
            "type": "error",
            "reason": f"Invalid collection name: {coll!r}",
        }
        return jsonify(stages)

    # Stage 3: Stemming
    expanded = _bot.process_search_query_further(qstack) if qstack else []
    stages["3_stemming"] = {
        "input_terms": qstack,
        "expanded_terms": expanded,
        "expansion_count": len(expanded) - len(qstack) if qstack else 0,
    }

    # Stage 4: User data lookup
    userdata = _bot.db.db.storage.find_one({"user_id": user_id})
    if not userdata:
        stages["4_user_lookup"] = {"error": "User not found in storage"}
        return jsonify(stages)

    user_collections = list(userdata.get("collection", {}).keys())
    coll_data = userdata.get("collection", {}).get(coll)
    if not coll_data:
        stages["4_user_lookup"] = {
            "error": f"Collection {coll!r} not found for user",
            "available_collections": user_collections,
        }
        return jsonify(stages)

    chatid = coll_data["id"]
    index_count = len(coll_data.get("index", []))
    fcaption = extra.get("caption", None)
    stages["4_user_lookup"] = {
        "chat_id": chatid,
        "collection_index_size": index_count,
        "available_collections": user_collections,
        "has_temp": userdata.get("temp") is not None,
        "forced_caption": fcaption,
    }

    # Stage 5: MongoDB aggregation
    pipeline = [
        {"$match": {"user_id": user_id}},
        {"$project": {"index": f"$collection.{coll}.index", "_id": 0}},
        {"$unwind": "$index"},
    ]
    if expanded:
        pipeline.append(
            {"$match": {"$or": [{"index.tags": {"$all": q}} for q in expanded]}}
        )
    pipeline.append({"$limit": 5})

    raw_results = list(_bot.db.db.storage.aggregate(pipeline))
    colls = list(
        (
            x["index"]["id"],
            x["index"]["tags"],
            x["index"].get("caption", None),
            x["index"].get("cache_stale", False),
        )
        for x in raw_results
    )
    stages["5_aggregation"] = {
        "pipeline": _safe_serialize(pipeline),
        "result_count": len(raw_results),
        "results": _safe_serialize(raw_results),
    }

    # Stage 6: Cache lookup + result building
    from telegram import (
        InlineQueryResultArticle,
        InputTextMessageContent,
    )

    final_results = [
        InlineQueryResultArticle(
            id=str(uuid4()),
            title=">> "
            + "|".join(
                (" ".join(q) for q in (expanded or [["Your Recent Selections"]]))
            ),
            input_message_content=InputTextMessageContent(
                "Search for `"
                + "|".join(" ".join(q) for q in expanded)
                + "' and more~"
                if len(expanded)
                else "Yes, these are your recents"
            ),
        )
    ]

    cache_info = []

    # If no query terms, show last_used
    if not qstack:
        last_used_data = _bot.db.db.storage.find_one(
            {"user_id": user_id},
            projection={"_id": 0, "collection": 0},
        )
        last_used = last_used_data.get("last_used", {}).get(coll, [])
        last_used_entries = []
        for msgid in last_used:
            msgid = int(msgid)
            cmsg = _bot.db.db.message_cache.find_one(
                {"$and": [{"chatid": chatid}, {"msg_id": msgid}]}
            )
            entry = {
                "msg_id": msgid,
                "cached": cmsg is not None,
                "data": _safe_serialize(cmsg) if cmsg else None,
            }
            if cmsg:
                cmsg["caption"] = fcaption
                clone = _bot.clone_messaage_with_data(cmsg, ["last", "used"])
                if clone:
                    final_results.append(clone)
                    entry["built_result"] = _serialize_inline_result(clone)
            last_used_entries.append(entry)
        stages["6_last_used"] = last_used_entries

        if len(final_results) > 1:
            stages["7_final_results"] = _serialize_results(final_results)
            page = extra.get("page", 0)
            start_index = min(page * 20, len(final_results))
            end_index = min(page * 20 + 20, len(final_results))
            if start_index == end_index:
                start_index, end_index = 0, 20
            stages["8_pagination"] = {
                "page": page,
                "start_index": start_index,
                "end_index": end_index,
                "total_results": len(final_results),
                "returned_results": _serialize_results(
                    final_results[start_index:end_index]
                ),
            }
            return jsonify(stages)

    # Build results from aggregation matches
    tempid = userdata.get("temp")
    tempid_val = tempid["id"] if tempid else None

    for col in colls:
        msg_id, tags_list, caption, cache_stale = col
        cmsg = _bot.db.db.message_cache.find_one(
            {"$and": [{"chatid": chatid}, {"msg_id": msg_id}]}
        )
        entry = {
            "msg_id": msg_id,
            "tags": tags_list,
            "default_caption": caption,
            "cache_stale": cache_stale,
            "cache_hit": cmsg is not None and not cache_stale,
        }
        if cmsg and not cache_stale:
            cmsg["caption"] = (
                fcaption
                if fcaption not in ["$def", "$default", "$"]
                else caption
            )
            entry["cached_data"] = _safe_serialize(cmsg)
            clone = _bot.clone_messaage_with_data(cmsg, tags_list)
            if clone:
                final_results.append(clone)
                entry["built_result"] = _serialize_inline_result(clone)
            else:
                entry["build_error"] = "clone_messaage_with_data returned None"
        else:
            entry["cached_data"] = _safe_serialize(cmsg) if cmsg else None
            entry["note"] = (
                "cache stale, would re-forward from source"
                if cache_stale
                else f"cache miss, would forward msg {msg_id} from chat {chatid} to temp {tempid_val}"
            )
            if not tempid_val:
                entry["note"] += " (BUT no temp chat set!)"

        cache_info.append(entry)

    stages["6_cache_lookup"] = cache_info

    # Stage 7: Final results
    stages["7_final_results"] = _serialize_results(final_results)

    # Stage 8: Pagination
    page = extra.get("page", 0)
    start_index = min(page * 20, len(final_results))
    end_index = min(page * 20 + 20, len(final_results))
    if start_index == end_index:
        start_index, end_index = 0, 20
    stages["8_pagination"] = {
        "page": page,
        "start_index": start_index,
        "end_index": end_index,
        "total_results": len(final_results),
        "returned_results": _serialize_results(
            final_results[start_index:end_index]
        ),
    }

    return jsonify(stages)


def _run_external_source_debug(stages, source_str, query, extra, user_id):
    """Actually execute the external source pipeline and capture all intermediates."""
    from anilist import (
        aniquery, squery_render, iquery_render, qquery_render,
        cquery_render, charquery_render, simple_query,
    )

    coll, *ireqs = source_str.split(":")
    ireqs = ":".join(ireqs)

    stages["4_source_dispatch"] = {
        "source_type": coll,
        "sub_request": ireqs or "(none)",
        "query": query,
        "extra": extra,
    }

    try:
        if coll == "anilist":
            _run_anilist_debug(stages, ireqs, query, extra, user_id)
        elif _bot.has_api(user_id, coll):
            _run_custom_api_debug(stages, coll, ireqs, query, extra, user_id)
        else:
            stages["5_error"] = {"error": f"Undefined source: {coll}"}
    except Exception as e:
        stages["error"] = {
            "exception": str(e),
            "traceback": traceback.format_exc(),
        }


def _run_anilist_debug(stages, ireqs, query, extra, user_id):
    """Execute anilist pipeline with full intermediate capture."""
    from anilist import (
        aniquery, squery_render, iquery_render, qquery_render,
        cquery_render, charquery_render, simple_query,
    )
    import json as json_mod

    if ireqs == "ql":
        # Raw GraphQL query
        stages["5_anilist_request"] = {
            "mode": "raw_graphql",
            "graphql_query": query,
        }
        try:
            raw_response = aniquery(query, {})
            stages["6_anilist_response"] = _safe_serialize(raw_response)
            result = [{
                "_type": "InlineQueryResultArticle",
                "title": "Raw request results for " + query,
                "input_message_content": {"message_text": json_mod.dumps(raw_response)},
            }]
            stages["7_final_results"] = result
        except Exception as e:
            stages["6_anilist_response"] = {"error": str(e), "traceback": traceback.format_exc()}

    elif ireqs == "aggql":
        match = re.search(r"^\`([^\`]+)\`\s*\`([^\`]+)\`$", query)
        stages["5_anilist_request"] = {
            "mode": "aggregate_graphql",
            "raw_query": query,
            "parsed": {"query": match.group(1), "aggregate": match.group(2)} if match else "PARSE_FAILED",
        }
        if match:
            q0, q1 = match.group(1, 2)
            try:
                raw_response = aniquery(q0, {})
                stages["6_anilist_response"] = _safe_serialize(raw_response)
                stages["6b_aggregate_pipeline"] = json_mod.loads(q1)
                # Actually run the aggregation
                temp_coll = _bot.db.client[f"temp_{user_id}"]
                db = temp_coll["temp"]
                db.insert_one(raw_response)
                agg_result = list(db.aggregate(json_mod.loads(q1)))
                db.drop()
                stages["7_aggregate_result"] = _safe_serialize(agg_result)
                stages["8_final_results"] = [{
                    "_type": "InlineQueryResultArticle",
                    "title": "Aggregate request results for " + q0,
                    "input_message_content": {"message_text": _safe_serialize(agg_result)},
                }]
            except Exception as e:
                stages["6_anilist_response"] = {"error": str(e), "traceback": traceback.format_exc()}

    elif ireqs in ("id", "one", "bychar", "char", ""):
        mode_map = {
            "id": ("by_id", iquery_render),
            "one": ("single_media", qquery_render),
            "bychar": ("by_character", cquery_render),
            "char": ("character_query", charquery_render),
            "": ("simple_search", squery_render),
        }
        mode_name, render_fn = mode_map[ireqs]

        # Capture what simple_query / aniquery would do
        stages["5_anilist_request"] = {
            "mode": mode_name,
            "query": query,
            "page": extra.get("page", 1),
        }

        # For simple search, show the GraphQL that would be sent
        if ireqs == "":
            stages["5_anilist_request"]["graphql_preview"] = {
                "type": "Page query",
                "variables": {"search": query, "page": extra.get("page", 1), "perPage": 5},
            }
        elif ireqs == "id":
            stages["5_anilist_request"]["graphql_preview"] = {
                "type": "Media(id:...) query",
                "constructed_query": f"id:{query}",
            }
        elif ireqs == "one":
            stages["5_anilist_request"]["graphql_preview"] = {
                "type": "Media(...) query",
                "constructed_query": query,
            }

        try:
            results = render_fn(query, extra)
            stages["6_raw_api_response"] = {
                "note": "Response already rendered into InlineQueryResults",
                "result_count": len(results),
            }
            stages["7_final_results"] = _serialize_results(results)
        except Exception as e:
            stages["6_raw_api_response"] = {"error": str(e), "traceback": traceback.format_exc()}
    else:
        stages["5_error"] = {
            "error": f"Unknown anilist sub-request: {ireqs}",
        }


def _run_custom_api_debug(stages, api_name, ireqs, query, extra, user_id):
    """Execute custom API pipeline with full intermediate capture at every IO stage."""
    handler = _bot.external_api_handler

    if api_name not in handler.apis:
        stages["5_error"] = {"error": f"API {api_name!r} not found"}
        return

    comm_type, inp, out, path = handler.apis[api_name]
    stages["5_api_definition"] = {
        "name": api_name,
        "comm_type": comm_type,
        "input_adapter": inp,
        "output_adapter": out,
        "path_template": path,
    }

    # Show input adapter definition
    if inp in handler.input_adapters:
        inpv = handler.input_adapters[inp]
        stages["5b_input_adapter_def"] = {
            "name": inp,
            "variable": inpv[0],
            "type": inpv[1],
            "body": inpv[2],
            "uses": inpv[3] if len(inpv) > 3 else None,
        }
    else:
        stages["5b_input_adapter_def"] = {"error": f"Undefined input adapter: {inp}"}
        return

    # Show output adapter definition
    if out in handler.output_adapters:
        outv = handler.output_adapters[out]
        stages["5c_output_adapter_def"] = {
            "name": out,
            "variable": outv[0],
            "type": outv[1],
            "body": outv[2],
            "uses": outv[3] if len(outv) > 3 else None,
        }
    else:
        stages["5c_output_adapter_def"] = {"error": f"Undefined output adapter: {out}"}
        return

    # Stage 6: Run input adapter
    stages["6_input_adapter_exec"] = {
        "input_value": query,
    }
    try:
        inpv = handler.input_adapters[inp]
        _type_in, q = handler.adapter(inp, inpv, query, env=dict(extra))
        from apihandler import NextStep
        if isinstance(q, NextStep):
            stages["6_input_adapter_exec"]["next_step_stripped"] = str(q.query)
            q = q.obj
        stages["6_input_adapter_exec"]["output_type"] = _type_in
        stages["6_input_adapter_exec"]["output_value"] = _safe_serialize(q) if not isinstance(q, str) else q
    except Exception as e:
        stages["6_input_adapter_exec"]["error"] = str(e)
        stages["6_input_adapter_exec"]["traceback"] = traceback.format_exc()
        return

    # Stage 7: HTTP request / comm execution
    try:
        resolved_path = path
        page_re = handler.page_re
        page_var_re = handler.page_var_re
        metavarre = handler.metavarre

        if "page" in extra:
            resolved_path = page_re.sub(
                lambda match: page_var_re(match.group(1)).sub(
                    str(extra["page"]), match.group(2)
                ),
                resolved_path,
            )
        else:
            resolved_path = page_re.sub("", resolved_path)

        stages["7_comm"] = {
            "comm_type": comm_type,
            "path_template": path,
        }

        if comm_type == "identity":
            result = metavarre.sub(q, resolved_path)
            stages["7_comm"]["resolved_path"] = resolved_path
            stages["7_comm"]["result"] = result

        elif comm_type == "http/link":
            resolved_path = metavarre.sub(urllib.parse.quote_plus(q), resolved_path)
            stages["7_comm"]["resolved_url"] = resolved_path
            stages["7_comm"]["result"] = resolved_path
            result = resolved_path

        elif comm_type == "json/post":
            resolved_path = metavarre.sub(q.get("pvalue", ""), resolved_path)
            body = json.dumps(q.get("value", {}))
            stages["7_comm"]["resolved_url"] = resolved_path
            stages["7_comm"]["post_body"] = json.loads(body)
            resp = http_requests.post(resolved_path, data=body, headers={"Content-Type": "application/json"})
            stages["7_comm"]["http_status"] = resp.status_code
            stages["7_comm"]["response_size"] = len(resp.content)
            result = resp.json()
            stages["7_comm"]["result"] = _safe_serialize(result)
            from apihandler import DotDict
            result = DotDict({"x": result}).x

        elif comm_type == "http/json":
            resolved_path = metavarre.sub(urllib.parse.quote_plus(q), resolved_path)
            stages["7_comm"]["resolved_url"] = resolved_path
            resp = http_requests.get(resolved_path)
            stages["7_comm"]["http_status"] = resp.status_code
            stages["7_comm"]["response_size"] = len(resp.content)
            result = resp.json()
            stages["7_comm"]["result"] = _safe_serialize(result)
            from apihandler import DotDict
            result = DotDict({"x": result}).x

        elif comm_type == "lit.http/json":
            resolved_path = metavarre.sub(q, resolved_path)
            stages["7_comm"]["resolved_url"] = resolved_path
            resp = http_requests.get(resolved_path)
            stages["7_comm"]["http_status"] = resp.status_code
            stages["7_comm"]["response_size"] = len(resp.content)
            result = resp.json()
            stages["7_comm"]["result"] = _safe_serialize(result)
            from apihandler import DotDict
            result = DotDict({"x": result}).x

        elif comm_type == "html/xpath":
            resolved_path = metavarre.sub(urllib.parse.quote_plus(q), resolved_path)
            stages["7_comm"]["resolved_url"] = resolved_path
            resp = http_requests.get(resolved_path)
            stages["7_comm"]["http_status"] = resp.status_code
            stages["7_comm"]["response_size"] = len(resp.content)
            stages["7_comm"]["response_preview"] = resp.text[:2000]
            from lxml import html as xhtml
            xml = xhtml.fromstring(resp.content)
            result = lambda x, xml=xml: xml.xpath(x)
            stages["7_comm"]["note"] = "Result is an xpath evaluator function"

        elif comm_type == "graphql":
            gql_body = {"query": q, "vars": {}}
            stages["7_comm"]["resolved_url"] = resolved_path
            stages["7_comm"]["graphql_body"] = gql_body
            resp = http_requests.post(resolved_path, json=gql_body)
            stages["7_comm"]["http_status"] = resp.status_code
            stages["7_comm"]["response_size"] = len(resp.content)
            result = resp.json()
            stages["7_comm"]["result"] = _safe_serialize(result)
            from apihandler import DotDict
            result = DotDict({"x": result}).x

        else:
            stages["7_comm"]["error"] = f"Unknown comm type: {comm_type}"
            return

    except Exception as e:
        stages["7_comm"]["error"] = str(e)
        stages["7_comm"]["traceback"] = traceback.format_exc()
        return

    # Stage 8: Run output adapter
    try:
        outv = handler.output_adapters[out]
        _type_out, rendered = handler.adapter(out, outv, result, env=dict(extra))
        stages["8_output_adapter_exec"] = {
            "output_type": _type_out,
            "rendered_items_count": len(rendered) if isinstance(rendered, (list, tuple)) else 1,
            "rendered_raw": _safe_serialize(rendered) if not isinstance(rendered, (list, tuple)) else [
                _safe_serialize(item) for item in rendered
            ],
        }
    except Exception as e:
        stages["8_output_adapter_exec"] = {
            "error": str(e),
            "traceback": traceback.format_exc(),
        }
        return

    # Stage 9: tgwrap - convert to InlineQueryResult objects
    try:
        tg_results = handler.tgwrap(api_name, _type_out, rendered)
        stages["9_final_results"] = _serialize_results(tg_results)
    except Exception as e:
        stages["9_final_results"] = {
            "error": str(e),
            "traceback": traceback.format_exc(),
        }


@debug_bp.route("/debug/user", methods=["POST"])
def debug_user():
    """Look up user storage data."""
    if not _check_auth():
        return jsonify({"error": "User lookup requires authentication"}), 403

    data = request.get_json(force=True)
    user_id = data.get("user_id")
    if not user_id:
        return jsonify({"error": "user_id is required"}), 400
    try:
        user_id = int(user_id)
    except ValueError:
        return jsonify({"error": "user_id must be an integer"}), 400

    userdata = _bot.db.db.storage.find_one({"user_id": user_id})
    if not userdata:
        return jsonify({"error": "User not found"}), 404

    collections = {}
    for name, cdata in userdata.get("collection", {}).items():
        collections[name] = {
            "chat_id": cdata.get("id"),
            "index_count": len(cdata.get("index", [])),
        }

    aliases = _bot.db.db.aliases.find_one({"user_id": user_id})

    result = {
        "user_id": user_id,
        "collections": collections,
        "has_temp": userdata.get("temp") is not None,
        "temp": _safe_serialize(userdata.get("temp")) if userdata.get("temp") else None,
        "last_used_keys": list(userdata.get("last_used", {}).keys()),
        "aliases": _safe_serialize(aliases.get("aliases", {})) if aliases else {},
        "used_count": userdata.get("used_count"),
    }

    return jsonify(result)


DEBUG_HTML = r"""<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Istorayje Debug Console</title>
<style>
* { box-sizing: border-box; margin: 0; padding: 0; }
body { font-family: 'Courier New', monospace; background: #0a0a0a; color: #e0e0e0; padding: 20px; }
h1 { color: #7fdbca; margin-bottom: 4px; font-size: 1.4em; }
h2 { color: #c792ea; margin: 16px 0 8px; font-size: 1.1em; }
.subtitle { color: #666; font-size: 0.85em; margin-bottom: 16px; }
.panel { background: #1a1a2e; border: 1px solid #333; border-radius: 6px; padding: 16px; margin-bottom: 12px; }
label { color: #82aaff; display: block; margin-bottom: 4px; font-size: 0.85em; }
input[type=text], input[type=number], input[type=password] {
    background: #0d1117; border: 1px solid #444; color: #e0e0e0; padding: 8px 10px;
    border-radius: 4px; width: 100%; font-family: inherit; font-size: 0.9em; margin-bottom: 8px;
}
input:focus { border-color: #7fdbca; outline: none; }
button {
    background: #1e3a5f; color: #7fdbca; border: 1px solid #7fdbca; padding: 8px 16px;
    border-radius: 4px; cursor: pointer; font-family: inherit; font-size: 0.9em; margin-right: 6px;
    margin-bottom: 6px;
}
button:hover { background: #2a4a6f; }
button.active { background: #7fdbca; color: #0a0a0a; }
.results { margin-top: 12px; }
.stage { background: #16213e; border: 1px solid #2a3a5e; border-radius: 4px; margin-bottom: 8px; overflow: hidden; }
.stage-header {
    background: #1a1a3e; padding: 8px 12px; cursor: pointer; display: flex;
    justify-content: space-between; align-items: center;
}
.stage-header:hover { background: #222255; }
.stage-name { color: #c792ea; font-weight: bold; }
.stage-summary { color: #666; font-size: 0.85em; }
.stage-body { padding: 12px; border-top: 1px solid #2a3a5e; }
.stage-body.collapsed { display: none; }
pre { background: #0d1117; padding: 10px; border-radius: 4px; overflow-x: auto; font-size: 0.82em; line-height: 1.4; white-space: pre-wrap; word-break: break-word; }
.tag { display: inline-block; background: #1e3a5f; color: #7fdbca; padding: 2px 6px; border-radius: 3px; font-size: 0.8em; margin: 1px 2px; }
.arrow { color: #c792ea; margin: 4px 0; font-size: 1.2em; text-align: center; }
.error { color: #f07178; }
.ok { color: #c3e88d; }
.warn { color: #ffcb6b; }
.grid { display: grid; grid-template-columns: 1fr 1fr; gap: 8px; }
@media (max-width: 600px) { .grid { grid-template-columns: 1fr; } }
.flex-row { display: flex; gap: 8px; align-items: end; }
.flex-row > * { flex: 1; }
.cache-entry { background: #0d1117; padding: 8px; border-radius: 4px; margin-bottom: 6px; border-left: 3px solid #444; }
.cache-entry.hit { border-left-color: #c3e88d; }
.cache-entry.miss { border-left-color: #f07178; }
.result-card { background: #0d1117; padding: 10px; border-radius: 4px; margin-bottom: 6px; border-left: 3px solid #82aaff; }
.result-card .result-type { color: #c792ea; font-size: 0.8em; font-weight: bold; }
.result-card .result-title { color: #e0e0e0; margin: 4px 0; }
.result-card .result-meta { color: #666; font-size: 0.8em; }
#status { color: #666; font-size: 0.85em; margin-top: 8px; }
.spinner { display: inline-block; width: 14px; height: 14px; border: 2px solid #333; border-top-color: #7fdbca; border-radius: 50%; animation: spin 0.6s linear infinite; }
@keyframes spin { to { transform: rotate(360deg); } }
.io-flow { display: flex; align-items: center; gap: 8px; flex-wrap: wrap; margin: 8px 0; }
.io-box { background: #1e3a5f; border: 1px solid #3a5a8f; border-radius: 4px; padding: 6px 10px; font-size: 0.85em; }
.io-arrow { color: #7fdbca; font-size: 1.2em; }
</style>
</head>
<body>
<h1>Istorayje Debug Console</h1>
<p class="subtitle">Inline query pipeline inspector &mdash; fires real requests through the pipeline</p>

<div class="panel">
    <div class="grid">
        <div>
            <label>Query String</label>
            <input type="text" id="query" placeholder="mycoll search terms {caption} | alternate">
        </div>
        <div class="flex-row">
            <div>
                <label>User ID</label>
                <input type="number" id="user_id" placeholder="123456789">
            </div>
            <div>
                <label>Password</label>
                <input type="password" id="pw" placeholder="optional">
            </div>
        </div>
    </div>
    <div>
        <button onclick="runParse()">Parse Only</button>
        <button onclick="runStem()">Parse + Stem</button>
        <button onclick="runSearch()">Full Pipeline</button>
        <button onclick="runUser()">User Info</button>
    </div>
    <div id="status"></div>
</div>

<div id="output" class="results"></div>

<script>
const $ = id => document.getElementById(id);

function pw() { return $('pw').value; }
function baseUrl() { return pw() ? `?pw=${encodeURIComponent(pw())}` : ''; }

function body() {
    const d = { query: $('query').value };
    const uid = $('user_id').value;
    if (uid) d.user_id = parseInt(uid);
    return d;
}

async function api(endpoint, data) {
    $('status').innerHTML = '<span class="spinner"></span> Running...';
    try {
        const r = await fetch(`/debug/${endpoint}${baseUrl()}`, {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(data),
        });
        const j = await r.json();
        $('status').textContent = r.ok ? 'Done' : `Error ${r.status}`;
        return j;
    } catch(e) {
        $('status').innerHTML = `<span class="error">Failed: ${e.message}</span>`;
        return null;
    }
}

function stageEl(name, summary, content, open=true) {
    const id = 's_' + Math.random().toString(36).slice(2);
    return `<div class="stage">
        <div class="stage-header" onclick="document.getElementById('${id}').classList.toggle('collapsed')">
            <span class="stage-name">${name}</span>
            <span class="stage-summary">${summary}</span>
        </div>
        <div id="${id}" class="stage-body${open ? '' : ' collapsed'}">${content}</div>
    </div>`;
}

function pre(obj) {
    if (obj === null || obj === undefined) return '<pre>null</pre>';
    return `<pre>${esc(JSON.stringify(obj, null, 2))}</pre>`;
}

function esc(s) {
    return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

function tags(arr) {
    return (arr||[]).map(t => `<span class="tag">${esc(t)}</span>`).join(' ');
}

function renderResultCard(r) {
    if (!r) return '';
    let meta = [];
    if (r.photo_file_id) meta.push('photo: ' + r.photo_file_id.slice(0,25) + '...');
    if (r.gif_file_id) meta.push('gif: ' + r.gif_file_id.slice(0,25) + '...');
    if (r.mpeg4_file_id) meta.push('mpeg4: ' + r.mpeg4_file_id.slice(0,25) + '...');
    if (r.document_file_id) meta.push('doc: ' + r.document_file_id.slice(0,25) + '...');
    if (r.voice_file_id) meta.push('voice: ' + r.voice_file_id.slice(0,25) + '...');
    if (r.audio_file_id) meta.push('audio: ' + r.audio_file_id.slice(0,25) + '...');
    if (r.sticker_file_id) meta.push('sticker: ' + r.sticker_file_id.slice(0,25) + '...');
    if (r.photo_url) meta.push('url: ' + r.photo_url);
    if (r.thumbnail_url) meta.push('thumb: ' + r.thumbnail_url);

    let content = '';
    if (r.input_message_content) {
        const imc = r.input_message_content;
        const text = imc.message_text || '';
        content = `<pre>${esc(text.slice(0, 500))}${text.length > 500 ? '...' : ''}</pre>`;
        if (imc.parse_mode) content = `<span class="tag">${esc(imc.parse_mode)}</span> ` + content;
    }

    let buttons = '';
    if (r.reply_markup && r.reply_markup.length) {
        buttons = '<p><b>Buttons:</b> ' + r.reply_markup.map(b =>
            `<span class="tag">${esc(b.text||'')}${b.url ? ' -> '+b.url : ''}${b.switch_inline_query_current_chat != null ? ' -> inline:'+b.switch_inline_query_current_chat : ''}</span>`
        ).join(' ') + '</p>';
    }

    return `<div class="result-card">
        <span class="result-type">${esc(r._type)}</span>
        ${r.title ? `<p class="result-title">${esc(r.title)}</p>` : ''}
        ${r.caption ? `<p class="result-meta">Caption: ${esc(r.caption)}</p>` : ''}
        ${meta.length ? `<p class="result-meta">${meta.map(m=>esc(m)).join(' | ')}</p>` : ''}
        ${content}
        ${buttons}
    </div>`;
}

function renderResultCards(results) {
    if (!results || !results.length) return '<i>No results</i>';
    return results.map((r, i) => `<p class="result-meta">#${i}</p>` + renderResultCard(r)).join('');
}

async function runParse() {
    const r = await api('parse', body());
    if (!r) return;
    let h = stageEl('Parse Result', `coll=${esc(r.collection)}`,
        `<p><b>Collection:</b> <span class="tag">${esc(r.collection)}</span></p>
         <p><b>Query terms:</b> ${r.query_terms.map(q => tags(q)).join(' <b>|</b> ') || '<i>none</i>'}</p>
         <p><b>Extra:</b> ${pre(r.extra)}</p>
         ${r.resolved_collection ? `<p><b>Resolved collection:</b> <span class="tag">${esc(r.resolved_collection)}</span>${r.resolved_collection !== r.collection ? ' <span class="warn">(aliased)</span>' : ''}</p>` : ''}
         ${r.implicit_collections ? `<p><b>Implicit collections:</b> ${pre(r.implicit_collections)}</p>` : ''}`
    );
    $('output').innerHTML = h;
}

async function runStem() {
    const r = await api('stem', body());
    if (!r) return;
    let stemHtml = '';
    if (r.individual_stems) {
        stemHtml = Object.entries(r.individual_stems).map(([t,s]) =>
            `<span class="tag">${esc(t)}</span> &rarr; <span class="tag">${esc(s)}</span>${t!==s?' <span class="warn">*</span>':''}`
        ).join('<br>');
    }
    let h = stageEl('Original Terms', `${(r.original_terms||[]).length} group(s)`,
        r.original_terms.map(q => tags(q)).join(' <b>|</b> ') || '<i>none</i>'
    );
    if (r.individual_stems) {
        h += stageEl('Stemming Map', '', stemHtml);
        h += stageEl('Expanded Terms', `${(r.expanded_terms||[]).length} variant(s)`,
            (r.expanded_terms||[]).map(q => tags(q)).join('<br>') || '<i>none</i>'
        );
    }
    if (r.note) h += `<p class="warn">${esc(r.note)}</p>`;
    $('output').innerHTML = h;
}

async function runSearch() {
    const r = await api('search', body());
    if (!r) return;
    let h = '';

    if (r.error) { $('output').innerHTML = `<p class="error">${esc(typeof r.error === 'string' ? r.error : JSON.stringify(r.error))}</p>`; return; }

    // Render all stages dynamically based on sorted keys
    const keys = Object.keys(r).sort();

    for (const key of keys) {
        const val = r[key];
        if (val === null || val === undefined) continue;

        // Format stage name from key
        const label = key.replace(/^\d+[a-z]?_/, '').replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
        const num = key.match(/^(\d+[a-z]?)/);
        const displayName = num ? `${num[1]}. ${label}` : label;

        // Special rendering for known stage types
        if (key === '1_parse') {
            h += stageEl(displayName, `coll=${esc(val.collection)}`,
                `<p><b>Collection:</b> <span class="tag">${esc(val.collection)}</span></p>
                 <p><b>Query:</b> ${(val.query_terms||[]).map(q => tags(q)).join(' | ') || '<i>none</i>'}</p>
                 <p><b>Extra:</b> ${pre(val.extra)}</p>`);
        }
        else if (key === '2_alias') {
            h += stageEl(displayName, val.changed ? 'aliased!' : 'unchanged',
                `<p>${esc(val.input)} &rarr; <span class="tag">${esc(val.resolved)}</span>
                 ${val.changed ? '<span class="warn"> (changed)</span>' : '<span class="ok"> (no alias)</span>'}</p>`);
        }
        else if (key === '3_routing') {
            h += stageEl(displayName, val.type,
                val.type === 'external_source'
                    ? `<p>External source: <span class="tag">${esc(val.source)}</span></p><p>Query: <code>${esc(val.remaining_query)}</code></p>`
                    : `<p class="error">${esc(val.reason || '')}</p>`);
        }
        else if (key === '3_stemming') {
            h += stageEl(displayName, `${val.expansion_count} new variant(s)`,
                `<p><b>Input:</b> ${(val.input_terms||[]).map(q=>tags(q)).join(' | ')}</p>
                 <p><b>Expanded (${(val.expanded_terms||[]).length}):</b><br>${(val.expanded_terms||[]).map(q=>tags(q)).join('<br>')}</p>`);
        }
        else if (key === '4_user_lookup') {
            if (val.error) {
                h += stageEl(displayName, 'error',
                    `<p class="error">${esc(val.error)}</p>
                     ${val.available_collections ? `<p>Available: ${val.available_collections.map(c=>`<span class="tag">${esc(c)}</span>`).join(' ')}</p>` : ''}`);
            } else {
                h += stageEl(displayName, `${val.collection_index_size} indexed`,
                    `<p><b>Chat ID:</b> ${val.chat_id}</p>
                     <p><b>Index size:</b> ${val.collection_index_size}</p>
                     <p><b>Has temp:</b> ${val.has_temp ? '<span class="ok">yes</span>' : '<span class="warn">no</span>'}</p>
                     ${val.forced_caption ? `<p><b>Forced caption:</b> <code>${esc(val.forced_caption)}</code></p>` : ''}
                     <p><b>Collections:</b> ${(val.available_collections||[]).map(c=>`<span class="tag">${esc(c)}</span>`).join(' ')}</p>`);
            }
        }
        else if (key === '4_source_dispatch') {
            const flow = `<div class="io-flow">
                <span class="io-box">query: "${esc(val.query)}"</span>
                <span class="io-arrow">&rarr;</span>
                <span class="io-box">@${esc(val.source_type)}${val.sub_request !== '(none)' ? ':'+esc(val.sub_request) : ''}</span>
            </div>`;
            h += stageEl(displayName, `@${val.source_type}`,
                flow + `${pre(val)}`);
        }
        else if (key === '5_aggregation') {
            h += stageEl(displayName, `${val.result_count} match(es)`,
                `<p><b>Pipeline:</b></p>${pre(val.pipeline)}
                 <p><b>Results:</b></p>${pre(val.results)}`, val.result_count > 0);
        }
        else if (key === '5_api_definition') {
            const flow = `<div class="io-flow">
                <span class="io-box">query</span>
                <span class="io-arrow">&rarr;</span>
                <span class="io-box">input: ${esc(val.input_adapter)}</span>
                <span class="io-arrow">&rarr;</span>
                <span class="io-box">${esc(val.comm_type)}: ${esc(val.path_template.slice(0,60))}</span>
                <span class="io-arrow">&rarr;</span>
                <span class="io-box">output: ${esc(val.output_adapter)}</span>
                <span class="io-arrow">&rarr;</span>
                <span class="io-box">results</span>
            </div>`;
            h += stageEl(displayName, val.comm_type, flow + pre(val));
        }
        else if (key.match(/^5[bc]_.*adapter_def$/)) {
            h += stageEl(displayName, val.name || '',
                `<p><b>Variable:</b> <code>${esc(val.variable||'')}</code></p>
                 <p><b>Type:</b> <span class="tag">${esc(val.type||'')}</span></p>
                 <p><b>Body:</b></p><pre>${esc(val.body||'')}</pre>
                 ${val.uses ? `<p><b>Uses:</b> ${pre(val.uses)}</p>` : ''}`);
        }
        else if (key === '6_input_adapter_exec') {
            const hasErr = val.error;
            h += stageEl(displayName, hasErr ? 'ERROR' : (val.output_type||''),
                `<p><b>Input:</b> <code>${esc(val.input_value||'')}</code></p>
                 ${hasErr
                    ? `<p class="error">${esc(val.error)}</p><pre>${esc(val.traceback||'')}</pre>`
                    : `<p><b>Output type:</b> <span class="tag">${esc(val.output_type||'')}</span></p>
                       <p><b>Output value:</b></p>${pre(val.output_value)}
                       ${val.next_step_stripped ? `<p class="warn">NextStep stripped: ${esc(val.next_step_stripped)}</p>` : ''}`
                 }`);
        }
        else if (key === '7_comm') {
            const hasErr = val.error;
            let commBody = `<p><b>Type:</b> <span class="tag">${esc(val.comm_type||'')}</span></p>`;
            if (val.resolved_url) commBody += `<p><b>URL:</b> <code>${esc(val.resolved_url)}</code></p>`;
            if (val.resolved_path) commBody += `<p><b>Path:</b> <code>${esc(val.resolved_path)}</code></p>`;
            if (val.post_body) commBody += `<p><b>POST body:</b></p>${pre(val.post_body)}`;
            if (val.graphql_body) commBody += `<p><b>GraphQL:</b></p>${pre(val.graphql_body)}`;
            if (val.http_status != null) commBody += `<p><b>HTTP status:</b> ${val.http_status} | <b>Size:</b> ${val.response_size} bytes</p>`;
            if (val.result) commBody += `<p><b>Response:</b></p>${pre(val.result)}`;
            if (val.response_preview) commBody += `<p><b>HTML preview:</b></p><pre>${esc(val.response_preview)}</pre>`;
            if (val.note) commBody += `<p class="warn">${esc(val.note)}</p>`;
            if (hasErr) commBody += `<p class="error">${esc(val.error)}</p><pre>${esc(val.traceback||'')}</pre>`;
            h += stageEl(displayName,
                val.http_status != null ? `HTTP ${val.http_status}` : (val.comm_type||''),
                commBody, true);
        }
        else if (key === '8_output_adapter_exec') {
            const hasErr = val.error;
            h += stageEl(displayName, hasErr ? 'ERROR' : `${val.rendered_items_count} items`,
                hasErr
                    ? `<p class="error">${esc(val.error)}</p><pre>${esc(val.traceback||'')}</pre>`
                    : `<p><b>Output type:</b> <span class="tag">${esc(val.output_type||'')}</span></p>
                       <p><b>Items:</b> ${val.rendered_items_count}</p>
                       ${pre(val.rendered_raw)}`);
        }
        else if (key === '6_cache_lookup') {
            if (Array.isArray(val)) {
                const hits = val.filter(c => c.cache_hit).length;
                h += stageEl(displayName, `${hits}/${val.length} hits`,
                    val.map(c => {
                        let inner = `<p><b>msg_id:</b> ${c.msg_id} ${tags(c.tags)}</p>`;
                        if (c.default_caption) inner += `<p><b>Default caption:</b> ${esc(c.default_caption)}</p>`;
                        if (c.cache_hit) {
                            inner += `<p><span class="ok">CACHE HIT</span></p>`;
                            if (c.cached_data) inner += pre(c.cached_data);
                        } else {
                            inner += `<p><span class="error">CACHE MISS</span></p>`;
                            if (c.note) inner += `<p class="warn">${esc(c.note)}</p>`;
                        }
                        if (c.cache_stale) inner += `<p><span class="warn">STALE</span></p>`;
                        if (c.built_result) {
                            inner += `<p><b>Built result:</b></p>` + renderResultCard(c.built_result);
                        }
                        if (c.build_error) inner += `<p class="error">${esc(c.build_error)}</p>`;
                        return `<div class="cache-entry ${c.cache_hit ? 'hit' : 'miss'}">${inner}</div>`;
                    }).join(''));
            } else {
                h += stageEl(displayName, '', pre(val));
            }
        }
        else if (key === '6_last_used') {
            if (Array.isArray(val)) {
                h += stageEl(displayName, `${val.length} entries`,
                    val.map(e => {
                        let inner = `<p><b>msg_id:</b> ${e.msg_id} - ${e.cached ? '<span class="ok">cached</span>' : '<span class="error">not cached</span>'}</p>`;
                        if (e.data) inner += pre(e.data);
                        if (e.built_result) {
                            inner += `<p><b>Built result:</b></p>` + renderResultCard(e.built_result);
                        }
                        return `<div class="cache-entry ${e.cached ? 'hit' : 'miss'}">${inner}</div>`;
                    }).join('') || '<i>none</i>');
            } else {
                h += stageEl(displayName, '', pre(val));
            }
        }
        else if (key.match(/final_results$/)) {
            if (Array.isArray(val)) {
                h += stageEl(displayName, `${val.length} result(s)`,
                    renderResultCards(val), true);
            } else if (val.error) {
                h += stageEl(displayName, 'ERROR',
                    `<p class="error">${esc(val.error)}</p><pre>${esc(val.traceback||'')}</pre>`);
            } else {
                h += stageEl(displayName, '', pre(val));
            }
        }
        else if (key.match(/pagination$/)) {
            h += stageEl(displayName, `page ${val.page}`,
                `<p>Page ${val.page}: items ${val.start_index}&ndash;${val.end_index} of ${val.total_results}</p>
                 ${val.returned_results ? '<p><b>Returned to user:</b></p>' + renderResultCards(val.returned_results) : ''}`);
        }
        else if (key === 'error') {
            h += stageEl('Error', '', `<p class="error">${esc(val.exception||'')}</p><pre>${esc(val.traceback||'')}</pre>`);
        }
        else {
            // Generic fallback for any other stage
            const summary = val.error ? 'ERROR' :
                val.result_count != null ? `${val.result_count} result(s)` :
                val.mode || '';
            h += stageEl(displayName, summary, pre(val));
        }
    }

    $('output').innerHTML = h;
}

async function runUser() {
    const r = await api('user', body());
    if (!r) return;
    if (r.error) { $('output').innerHTML = `<p class="error">${esc(r.error)}</p>`; return; }

    let h = stageEl('User Info', `${Object.keys(r.collections).length} collection(s)`,
        `<p><b>User ID:</b> ${r.user_id}</p>
         <p><b>Has temp:</b> ${r.has_temp ? '<span class="ok">yes</span>' : '<span class="warn">no</span>'}</p>
         ${r.temp ? `<p><b>Temp:</b> ${pre(r.temp)}</p>` : ''}
         <p><b>Used count:</b> ${r.used_count ?? 'N/A'}</p>`);

    h += stageEl('Collections', `${Object.keys(r.collections).length}`,
        Object.entries(r.collections).map(([name, d]) =>
            `<p><span class="tag">${esc(name)}</span> chat_id=${d.chat_id} index=${d.index_count}</p>`
        ).join(''));

    if (Object.keys(r.aliases).length) {
        h += stageEl('Aliases', `${Object.keys(r.aliases).length}`,
            Object.entries(r.aliases).map(([a,v]) =>
                `<p><span class="tag">${esc(a)}</span> &rarr; <span class="tag">${esc(v)}</span></p>`
            ).join(''));
    }

    if (r.last_used_keys.length) {
        h += stageEl('Last Used Keys', '', r.last_used_keys.map(k => `<span class="tag">${esc(k)}</span>`).join(' '));
    }

    $('output').innerHTML = h;
}

// Enter to submit
$('query').addEventListener('keydown', e => { if (e.key === 'Enter') runSearch(); });
</script>
</body>
</html>
"""
