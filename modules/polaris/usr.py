from fastapi import APIRouter, Request, Response
from core_common import core_process_request, core_prepare_response, E
from core_database import get_db
from tinydb import Query, where
import random
import time

router = APIRouter(prefix="/polaris/usr", tags=["usr"])
router.model_whitelist = ["LAV", "XIF"]

@router.post("")
@router.post("/")
@router.post("/{path:path}")
async def polaris_usr_dispatch(request: Request):
    try:
        request_info = await core_process_request(request)
        method = request_info["method"]
        
        func_name = f"polaris_usr_{method}"
        if func_name in globals():
            return await globals()[func_name](request)
        else:
            print(f"USR Dispatch: Function {func_name} not found")
            return Response(status_code=404)
    except Exception as e:
        import traceback
        with open("debug_log.txt", "w") as f:
            f.write(traceback.format_exc())
            f.write(f"\nLast known step: Dispatcher Failed")
        print(traceback.format_exc())
        return Response(status_code=500)

print("Loaded modules/polaris/usr.py")

def get_profile(dataid, refid=None):
    db = get_db().table("polaris_profile")
    
    # helper for card lookup
    def lookup_card(cid):
        p = db.get(where("card") == cid)
        if p: return p
        p = db.get(where("card") == cid.strip())
        if p: return p
        return None

    # Try dataid (card)
    if dataid:
        p = lookup_card(dataid)
        if p: return p
        
    # Try refid
    if refid:
        p = db.get(where("refid") == refid)
        if p: return p
        p = db.get(where("refid") == refid.strip())
        if p: return p

    # Brute force search for stripped equality on CARD only (refid usually strict)
    if dataid:
        clean_id = dataid.strip()
        for profile in db.all():
            if profile.get("card", "").strip() == clean_id:
                return profile
            
    return None

async def polaris_usr_sign_up(request: Request):
    try:
        request_info = await core_process_request(request)
        root = request_info["root"]
        
        # Robust ID Extraction (similar to get)
        dataid = None
        refid = None
        name = "PLAYER"
        
        if len(root) > 0:
            usr_node = root[0]
            for child in usr_node:
                if child.tag == "data_id": dataid = child.text
                if child.tag == "ref_id": refid = child.text
                if child.tag == "usr_name": name = child.text

        if not dataid and "srcid" in root.attrib:
            dataid = root.attrib["srcid"]
            
        if not dataid:
            print("polaris_usr_sign_up: Error - data_id missing")
            return Response(status_code=400)

        # Normalize IDs
        dataid = str(dataid).strip()
        if refid: refid = str(refid).strip()
        name = str(name).strip() if name else "PLAYER"

        print(f"polaris_usr_sign_up: Processing signup for card='{dataid}' name='{name}'")
        
        db = get_db().table("polaris_profile")
        profile = db.get(where("card") == dataid)
        
        if not profile:
            print(f"polaris_usr_sign_up: Creating NEW profile")
            profile = {"card": dataid, "version": {}}
        
        # Ensure Critical Fields
        if "refid" not in profile and refid: profile["refid"] = refid
        if "dataid" not in profile: profile["dataid"] = dataid
        if "name" not in profile: profile["name"] = name
        
        # Integers
        if "usr_id" not in profile: profile["usr_id"] = random.randint(100000, 999999)
        
        # Strings
        if "crew_id" not in profile: profile["crew_id"] = f"{random.randint(0,999999999999):012d}"

        db.upsert(profile, where("card") == dataid)
        print(f"polaris_usr_sign_up: Saved. usr_id={profile['usr_id']}")
        
        # Response (Matches SignUpModeler.cs)
        response = E.response(
            E.usr(
                E.usr_id(int(profile["usr_id"]), __type="s32"),
                E.crew_id(str(profile["crew_id"]), __type="str")
            )
        )
        
        response_body, response_headers = await core_prepare_response(request, response)
        return Response(content=response_body, headers=response_headers)

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return Response(status_code=500)

async def polaris_usr_get(request: Request):
    try:
        request_info = await core_process_request(request)
        
        # ID Extraction Logic (Unchanged as it works)
        root = request_info["root"]
        usr_node = root[0]
        dataid = None
        for child in usr_node:
            if child.tag == "data_id":
                dataid = child.text
                break
            if child.tag == "ref_id" and dataid is None:
                dataid = child.text
        if not dataid: dataid = root.attrib["srcid"]
            
        p = get_profile(dataid, dataid)

        if not p or "name" not in p:
            print(f"polaris_usr_get: Profile NOT found for card={dataid}")
            response = E.response(E.usr(E.result(1, __type="s32")))
            response_body, response_headers = await core_prepare_response(request, response)
            return Response(content=response_body, headers=response_headers)
            
        print(f"polaris_usr_get: Generating response for {p.get('name')}")

        # --- Data Preparation & Corrections ---
        
        # 1. Sync Tutorial Flags
        # Client needs tutorial_skipped=1 if cleared, but never sends it. Explicitly sync.
        # Also sync Gacha Ticket as it often signals tutorial completion.
        
        def safe_bool(k):
            val = p.get(k)
            if str(val).lower() == "true": return 1
            try: return 1 if int(val or 0) else 0
            except: return 0

        p_cleared = safe_bool("is_tutorial_cleared")
        p_skipped = safe_bool("tutorial_skipped")
        p_gacha = safe_bool("gacha_ticket_received")
        
        # [Refined Logic]
        # Cleared=1 (Played), Skipped=1 (Skipped). distinct states.
        # Fallback: If Gacha Ticket exists, user is definitely done -> Assume Cleared.
        
        is_cleared = 1 if (p_cleared or p_gacha) else 0
        
        # 3. Safe Sub-Object Access (Moved UP for calculation)
        play_info = p.get("play_info") or {}
        main_opt = p.get("main_option") or {}
        counts = p.get("counts") or {}
        
        # Helper for Play Info (Strict Types & Crash Proof)
        def pi_s(k, d=""): return str(play_info.get(k, d))
        def pi_i(k, d=0):
            val = play_info.get(k, d)
            try: return int(val or 0)
            except: return 1 if str(val).lower() == "true" else 0

        is_skipped = 1 if p_skipped else 0
        gacha_ticket = 1 if p_gacha else 0

        # Helper for Main Option
        def mo_s(k, d=""): return str(main_opt.get(k, d))
        def mo_i(k, d=0):
            val = main_opt.get(k, d)
            try: return int(val or 0)
            except: return 1 if str(val).lower() == "true" else 0
        def mo_b(k, d=0):
            val = main_opt.get(k, d)
            if str(val).lower() == "true": return True
            try: return bool(int(val or 0))
            except: return False 

        # --- XML Construction (Strict Schema Order) ---
        response = E.response(
            E.usr(
                E.result(0, __type="s32"),
                E.now_date(time.strftime("%Y-%m-%d %H:%M:%S"), __type="str"),
                E.usr_id(int(p.get("usr_id", 0)), __type="s32"),
                E.crew_id(str(p.get("crew_id", "0")), __type="str"),
                E.gacha_ticket_received(gacha_ticket, __type="s32"),
                E.tutorial_skipped(is_skipped, __type="s32"),
                
                E.usr_profile(
                    E.usr_name(str(p.get("name", "PLAYER")), __type="str"),
                    E.usr_rank(int(p.get("rank", 1)), __type="s32"),
                    E.exp(int(p.get("exp", 0)), __type="s32"),
                    E.comment(str(p.get("comment", "")), __type="str"),
                    E.is_tutorial_cleared(bool(is_cleared), __type="bool"),
                ),
                
                E.usr_play_info(
                    E.softcode(pi_s("softcode"), __type="str"),
                    E.asset_version(pi_i("asset_version"), __type="s32"),
                    E.start_date(pi_s("start_date"), __type="str"),
                    E.end_date(pi_s("end_date"), __type="str"),
                    E.play_days(pi_i("play_days"), __type="s32"),
                    E.consecutive_days(pi_i("consecutive_days"), __type="s32"),
                    E.consecutive_weeks(pi_i("consecutive_weeks"), __type="s32"),
                    E.last_play_week(pi_s("last_play_week"), __type="str"),
                    E.today_play_count(pi_i("today_play_count"), __type="s32"),
                    E.mode_id(pi_i("mode_id"), __type="s32"),
                    E.music_id(pi_i("music_id", 3), __type="s32"),
                    E.folder_id(pi_i("folder_id", 1), __type="s32"),
                    E.chart_difficulty_type(pi_i("chart_difficulty_type"), __type="s32"),
                    E.pcb_id(pi_s("pcb_id"), __type="str"),
                    E.loc_id(pi_s("loc_id"), __type="str"),
                    E.shop_name(pi_s("shop_name"), __type="str"),
                    E.beginner_play_count(pi_i("beginner_play_count"), __type="s32"),
                    E.standard_play_count(pi_i("standard_play_count"), __type="s32"),
                    E.freetime4_play_count(pi_i("freetime4_play_count"), __type="s32"),
                    E.freetime6_play_count(pi_i("freetime6_play_count"), __type="s32"),
                    E.freetime8_play_count(pi_i("freetime8_play_count"), __type="s32"),
                    E.freetime12_play_count(pi_i("freetime12_play_count"), __type="s32"),
                    E.local_matching_play_count(pi_i("local_matching_play_count"), __type="s32"),
                    E.global_matching_play_count(pi_i("global_matching_play_count"), __type="s32"),
                    E.freetime_play_count(pi_i("freetime_play_count"), __type="s32"),
                    E.freetime_play_total_time(pi_i("freetime_play_total_time"), __type="s32"),
                ),

                E.usr_main_option(
                    E.notes_design_type(mo_i("notes_design_type"), __type="s32"),
                    E.tap_se_type(mo_i("tap_se_type"), __type="s32"),
                    E.tap_effect_type(mo_i("tap_effect_type"), __type="s32"),
                    E.right_fader_color(mo_i("right_fader_color"), __type="s32"),
                    E.left_fader_color(mo_i("left_fader_color"), __type="s32"),
                    E.chart_option(mo_i("chart_option"), __type="s32"),
                    E.high_speed(mo_i("high_speed"), __type="s32"),
                    E.notes_display_timing(mo_i("notes_display_timing"), __type="s32"),
                    E.judge_timing(mo_i("judge_timing"), __type="s32"),
                    E.judge_display_position(mo_i("judge_display_position"), __type="s32"),
                    E.display_fast_slow(mo_i("display_fast_slow"), __type="s32"),
                    E.lane_alpha(mo_i("lane_alpha"), __type="s32"),
                    E.movie_brightness(mo_i("movie_brightness"), __type="s32"),
                    E.skill_cut_in(mo_i("skill_cut_in"), __type="s32"),
                    E.is_voice_active(mo_b("is_voice_active"), __type="bool"),
                    E.combo_special_display(mo_i("combo_special_display"), __type="s32"),
                    E.music_volume(mo_i("music_volume"), __type="s32"),
                    E.se_volume(mo_i("se_volume"), __type="s32"),
                    E.voice_volume(mo_i("voice_volume"), __type="s32"),
                    E.out_game_music_volume(mo_i("out_game_music_volume"), __type="s32"),
                    E.out_game_se_volume(mo_i("out_game_se_volume"), __type="s32"),
                    E.out_game_voice_volume(mo_i("out_game_voice_volume"), __type="s32"),
                    E.master_volume(mo_i("master_volume"), __type="s32"),
                    E.headphone_volume(mo_i("headphone_volume"), __type="s32"),
                    E.bass_shaker_volume(mo_i("bass_shaker_volume"), __type="s32"),
                    E.force_open_prev_in_game_option(mo_b("force_open_prev_in_game_option"), __type="bool"),
                    E.display_bar_line(mo_i("display_bar_line"), __type="s32"),
                    E.bga_id(mo_s("bga_id"), __type="str"),
                ),

                E.usr_privacy(
                     E.disp_name_to_other(1, __type="s32"),
                     E.disp_shop_to_other(1, __type="s32"),
                     E.disp_shop_to_me(1, __type="s32"),
                     E.disp_skill_to_other(1, __type="s32"),
                     E.disp_skill_to_me(1, __type="s32")
                ),
                E.usr_nametag(
                     E.nametag_badge1_id(str(main_opt.get("nametag_badge1_id", "0")), __type="str"),
                     E.nametag_badge2_id(str(main_opt.get("nametag_badge2_id", "0")), __type="str"),
                     E.nametag_badge3_id(str(main_opt.get("nametag_badge3_id", "0")), __type="str"),
                     E.nametag_plate_id(str(main_opt.get("nametag_plate_id", "0")), __type="str"),
                     E.nametag_title_id(str(main_opt.get("nametag_title_id", "0")), __type="str"),
                     E.set_title_name(str(main_opt.get("set_title_name", "")), __type="str"),
                     E.set_title_rarity(str(main_opt.get("set_title_rarity", "0")), __type="str")
                ),
                E.usr_sort_setting(
                     E.musicselect_sort(0, __type="s32"),
                     E.musicselect_filter(0, __type="s32"),
                     E.musicselect_order(0, __type="s32"),
                     E.character_training_list_sort(0, __type="s32"),
                     E.character_training_list_filter(0, __type="s32"),
                     E.character_training_list_order(0, __type="s32"),
                     E.character_replacement_list_sort(0, __type="s32"),
                     E.character_replacement_list_filter(0, __type="s32"),
                     E.character_replacement_list_order(0, __type="s32"),
                     E.character_material_list_sort(0, __type="s32"),
                     E.character_material_list_filter(0, __type="s32"),
                     E.character_material_list_order(0, __type="s32")
                ),
                E.usr_unlock_music(
                    *[ E.music(E.music_id(music_id, __type="s32"), E.chart_difficulty_type(diff, __type="s32"), E.unlock_type(0, __type="s32")) 
                       for music_id in list(range(1, 286)) + [99900, 99901] for diff in range(5) ]
                ),
                E.usr_item(
                     *[ E.item(E.item_id(f"chart.{music_id}.{diff}", __type="str"), E.count(1, __type="s32"), E.income(0, __type="s32"), E.expense(0, __type="s32"))
                        for music_id in list(range(1, 286)) + [99900, 99901] for diff in range(5) ]
                ),
                E.usr_name_titles(),
                E.usr_deck(
                    *[
                        E.deck(
                            E.deck_number(int(d.get("deck_number", 0)), __type="s32"),
                            E.is_main(bool(int(d.get("is_main", 0))), __type="bool"),
                            E.is_select(bool(int(d.get("is_select", 0))), __type="bool"),
                            E.deck_name(d.get("deck_name", "DECK 1"), __type="str"),
                            E.contenter_index(d.get("contenter_index", "1"), __type="str"),
                            E.supportsnap1_index(d.get("supportsnap1_index", "0"), __type="str"),
                            E.supportsnap2_index(d.get("supportsnap2_index", "0"), __type="str"),
                            E.supportsnap3_index(d.get("supportsnap3_index", "0"), __type="str"),
                            E.supportsnap4_index(d.get("supportsnap4_index", "0"), __type="str"),
                            E.frame_id(d.get("frame_id", ""), __type="str"),
                            E.pose_id(d.get("pose_id", ""), __type="str")
                        ) for d in p.get("decks", [])
                    ]
                ),
                E.usr_character_card(),
                E.usr_character(),
                E.usr_login_bonus(),
                E.usr_music_mission(),
                E.usr_extend_music_mission(),
                E.usr_count(
                    *[ E.count(E.key(k, __type="str"), E.value(int(v), __type="s32")) for k, v in counts.items() ]
                ),
                E.usr_chatstamp(),
                E.usr_action_count(),
                E.pa_skill(
                     E.pa_skill_history(),
                     E.pa_skill_history_index(0, __type="s32"),
                     E.skill(0, __type="s32")
                )
            )
        )

        response_body, response_headers = await core_prepare_response(request, response)
        return Response(content=response_body, headers=response_headers)

    except Exception as e:
        import traceback
        print(traceback.format_exc())
        with open("debug_log.txt", "w") as f:
            f.write(traceback.format_exc())
            f.write(f"\nLast known step: Response Generation Failed")
        return Response(status_code=500)

async def polaris_usr_save(request: Request):
    request_info = await core_process_request(request)
    root = request_info["root"][0]
    
    try:
        usr_id = int(root.find("usr_id").text)
        print(f"polaris_usr_save: usr_id={usr_id}")
    except Exception as e:
        print(f"polaris_usr_save: Failed to get usr_id: {e}")
        return Response(status_code=400)
    
    db = get_db().table("polaris_profile")
    profile = db.get(where("usr_id") == usr_id)
    
    if profile:
        print(f"polaris_usr_save: Profile found for usr_id={usr_id}")
        
        # Helper to extract text safely
        def get_text(node, tag, default=""):
            child = node.find(tag)
            return child.text if child is not None else default

        def get_int(node, tag, default=0):
            child = node.find(tag)
            if child is None or not child.text: return default
            try:
                return int(child.text)
            except ValueError:
                # Fallback for boolean-like strings if necessary, though get_bool matches those
                return default

        def get_bool(node, tag, default=0):
            child = node.find(tag)
            if child is None or not child.text: return default
            val = child.text.lower()
            if val in ["true", "1"]: return 1
            if val in ["false", "0"]: return 0
            try: return int(val)
            except: return default

        # --- Update Root Fields ---
        # tutorial_skipped is NOT in SavePlayData schema, so we can't read it here.
        # But gacha_ticket_received IS.
        if root.find("gacha_ticket_received") is not None:
            profile["gacha_ticket_received"] = get_int(root, "gacha_ticket_received")

        # --- Update Profile ---
        usr_profile = root.find("usr_profile")
        if usr_profile is not None:
            if usr_profile.find("usr_name") is not None:
                profile["name"] = get_text(usr_profile, "usr_name")
            if usr_profile.find("is_tutorial_cleared") is not None:
                val = get_bool(usr_profile, "is_tutorial_cleared")
                profile["is_tutorial_cleared"] = val
                print(f"polaris_usr_save: Updated is_tutorial_cleared={val}")
            
            if usr_profile.find("usr_rank") is not None: profile["rank"] = get_int(usr_profile, "usr_rank")
            if usr_profile.find("exp") is not None: profile["exp"] = get_int(usr_profile, "exp")
            if usr_profile.find("comment") is not None: profile["comment"] = get_text(usr_profile, "comment")

        # --- Update Play Info ---
        usr_play_info = root.find("usr_play_info")
        if usr_play_info is not None:
            if "play_info" not in profile: profile["play_info"] = {}
            pi = profile["play_info"]
            
            # Non-counter fields: Update directly
            pi["softcode"] = get_text(usr_play_info, "softcode")
            pi["asset_version"] = get_int(usr_play_info, "asset_version")
            pi["start_date"] = get_text(usr_play_info, "start_date")
            pi["end_date"] = get_text(usr_play_info, "end_date")
            pi["play_days"] = get_int(usr_play_info, "play_days")
            pi["consecutive_days"] = get_int(usr_play_info, "consecutive_days")
            pi["consecutive_weeks"] = get_int(usr_play_info, "consecutive_weeks")
            pi["last_play_week"] = get_text(usr_play_info, "last_play_week")
            pi["today_play_count"] = get_int(usr_play_info, "today_play_count")
            
            pi["mode_id"] = get_int(usr_play_info, "mode_id")
            pi["music_id"] = get_int(usr_play_info, "music_id")
            pi["folder_id"] = get_int(usr_play_info, "folder_id")
            pi["chart_difficulty_type"] = get_int(usr_play_info, "chart_difficulty_type")
            pi["pcb_id"] = get_text(usr_play_info, "pcb_id")
            pi["loc_id"] = get_text(usr_play_info, "loc_id")
            pi["shop_name"] = get_text(usr_play_info, "shop_name")
            
            # Helper to update counts monotonically (MAX strategy)
            def update_count(field):
                req_val = get_int(usr_play_info, field)
                old_val = pi.get(field, 0)
                try: old_val = int(old_val)
                except: old_val = 0
                pi[field] = max(req_val, old_val)

            update_count("beginner_play_count")
            update_count("standard_play_count")
            update_count("freetime4_play_count")
            update_count("freetime6_play_count")
            update_count("freetime8_play_count")
            update_count("freetime12_play_count")
            update_count("local_matching_play_count")
            update_count("global_matching_play_count")
            update_count("freetime_play_count")
            # Time is cumulative, safe to take max
            update_count("freetime_play_total_time")
            
            # [Fix] Apply game_play_count delta from Action Log to specific mode counter
            # If client snapshot was 0 but log says +1, this ensures we count it.
            usr_action_log = root.find("usr_action_count_change_log")
            game_play_delta = 0
            if usr_action_log is not None:
                for action in usr_action_log.findall("action_log"):
                     if get_text(action, "key") == "game_play_count":
                         game_play_delta += get_int(action, "change_count")
            
            if game_play_delta > 0:
                mid = pi.get("mode_id", 0)
                print(f"polaris_usr_save: ActionLog Delta +{game_play_delta} for Mode {mid}")
                
                # Mapping ModeID -> Field
                mode_field = None
                if mid == 10: mode_field = "standard_play_count"
                elif mid == 20: mode_field = "freetime6_play_count" 
                elif mid == 21: mode_field = "freetime8_play_count"
                elif mid == 23: mode_field = "freetime12_play_count"
                elif mid == 30: mode_field = "local_matching_play_count"
                elif mid == 40: mode_field = "global_matching_play_count"
                
                def safe_add(field, delta):
                    try: val = int(pi.get(field, 0))
                    except: val = 0
                    pi[field] = val + delta

                if mode_field:
                    safe_add(mode_field, game_play_delta)
                    if mid in [20, 21, 23]:
                        safe_add("freetime_play_count", game_play_delta)
                
                # Ensure Total count tracks
                # (Client might compute total sum, but we should be consistent)

            print(f"polaris_usr_save: [DEBUG] Play Counts Update -> "
                  f"Std={pi.get('standard_play_count')} "
                  f"F4={pi.get('freetime4_play_count')} "
                  f"F6={pi.get('freetime6_play_count')} "
                  f"F8={pi.get('freetime8_play_count')} "
                  f"F12={pi.get('freetime12_play_count')} "
                  f"TotalTimer={pi.get('freetime_play_count')}")

        # --- Generic Updates ---
        for tag, key in [("usr_main_option", "main_option"), 
                         ("usr_privacy", "privacy"), 
                         ("usr_nametag", "nametag"), 
                         ("usr_sort_setting", "sort_setting")]:
            node = root.find(tag)
            if node is not None:
                if key not in profile: profile[key] = {}
                for child in node:
                    profile[key][child.tag] = child.text

        # --- Update Unlocked Music ---
        usr_unlock_music = root.find("usr_unlock_music")
        if usr_unlock_music is not None:
            if "unlock_music" not in profile: profile["unlock_music"] = []
            current_unlocks = {}
            for u in profile["unlock_music"]:
                if isinstance(u, dict) and "music_id" in u:
                    current_unlocks[u["music_id"]] = u

            for item in usr_unlock_music.findall("music"):
                mid = get_int(item, "music_id")
                entry = {
                    "music_id": mid,
                    "unlock_phase": get_int(item, "unlock_phase"),
                    "can_buying": get_int(item, "can_buying"),
                    "is_new": get_int(item, "is_new"),
                    "use_item_id": get_int(item, "use_item_id"),
                    "use_item_num": get_int(item, "use_item_num")
                }
                
                if mid in current_unlocks:
                    current_unlocks[mid].update(entry)
                else:
                    profile["unlock_music"].append(entry)
                    current_unlocks[mid] = entry

        # --- Update Items (Change Log & List) ---
        # Note: Client sends usr_item_change_log usually, or usr_item list.
        # We should handle change log to update quantities if provided.
        usr_item_change_log = root.find("usr_item_change_log")
        if usr_item_change_log is not None:
             if "items" not in profile: profile["items"] = []
             # This simple logic assumes items list exists. 
             # Ideally we need a full inventory system. For now, let's just log or append if specific list is sent.
             # The client log showed "usr_item_change_log", so we should process it if possible, 
             # but often just saving the full list from usr_item (if sent) is easier.
             # If usr_item is NOT sent, we might be missing inventory sync.
             pass

        usr_item = root.find("usr_item")
        if usr_item is not None:
            if "items" not in profile: profile["items"] = []
            current_items = {}
            for i in profile["items"]:
                if isinstance(i, dict) and "item_id" in i and "item_type" in i:
                     current_items[(i["item_id"], i["item_type"])] = i

            for item in usr_item.findall("item"):
                iid = get_int(item, "item_id")
                itype = get_int(item, "item_type")
                entry = {
                    "item_id": iid,
                    "item_type": itype,
                    "is_new": get_int(item, "is_new"),
                    "limit_date": get_text(item, "limit_date"),
                    "remain_time": get_int(item, "remain_time"),
                    "param": get_int(item, "param")
                }
                key = (iid, itype)
                if key in current_items:
                    current_items[key].update(entry)
                else:
                    profile["items"].append(entry)
                    current_items[key] = entry

        # --- Update Decks ---
        usr_deck = root.find("usr_deck")
        if usr_deck is not None:
            profile["decks"] = [] # valid strategy to replace full list if client sends all
            for deck in usr_deck.findall("deck"):
                d = {}
                for child in deck:
                    d[child.tag] = child.text
                profile["decks"].append(d)

        # --- Update Characters ---
        # "usr_character_card" AND "usr_character"
        usr_dummy = root.find("usr_character_card") # Sometimes empty tags
        
        # --- Update Music Missions ---
        usr_music_mission = root.find("usr_music_mission")
        if usr_music_mission is not None:
            if "music_missions" not in profile: profile["music_missions"] = []
            # Replace or merge? Strategy: Replace usually safe for lists sent in full
            # But XML log shows "usr_music_mission" empty tag?
            # If it has children, parse.
            if len(usr_music_mission) > 0:
                profile["music_missions"] = []
                for mm in usr_music_mission.findall("music_mission"):
                    m = {}
                    for child in mm:
                       m[child.tag] = child.text
                    profile["music_missions"].append(m)

        # --- Update PA Skill ---
        pa_skill = root.find("pa_skill")
        if pa_skill is not None:
            if "pa_skill" not in profile: profile["pa_skill"] = {}
            for child in pa_skill:
                profile["pa_skill"][child.tag] = child.text

        # --- Update Action Change Log (Play Counts Logic) ---
        usr_action_log = root.find("usr_action_count_change_log")
        if usr_action_log is not None:
            if "action_logs" not in profile: profile["action_logs"] = [] # Audit logs
            
            for action in usr_action_log.findall("action_log"):
                key = get_text(action, "key")
                count = get_int(action, "change_count")
                # Store log?
                # profile["action_logs"].append({"key": key, "change": count, "ts": time.time()})
                
                # Update counters if they exist in generic counts
                if "counts" not in profile: profile["counts"] = {}
                cur = profile["counts"].get(key, 0)
                profile["counts"][key] = cur + count

                # SPECIAL: game_play_count mapping
                if key == "game_play_count":
                    print(f"polaris_usr_save: ActionLog 'game_play_count' +{count}")
                    # If we need to update standard_play_count in play_info as well?
                    # Usually play_info is the snapshot. 
                    # If client sent snapshot 0 but log +1, we trust the log?
                    # But client snapshot should be correct next time if we save generic count.
                    pass

        # --- Update Generic Counts ---
        usr_count = root.find("usr_count")
        if usr_count is not None:
            if "counts" not in profile: profile["counts"] = {}
            for count_item in usr_count.findall("count"):
                key_node = count_item.find("key")
                val_node = count_item.find("value") # [Fix] Tag provided by client is "value"
                if key_node is not None and val_node is not None:
                    try:
                        profile["counts"][key_node.text] = int(val_node.text)
                    except: pass
        
        # --- PERSIST TO DB ---
        db.upsert(profile, where("usr_id") == usr_id)
        print(f"polaris_usr_save: Profile SAVED to DB for usr_id={usr_id}")

    else:
        print(f"polaris_usr_save: Profile NOT found for usr_id={usr_id}")

    response = E.response(
        E.usr(
            E.now_date(time.strftime("%Y-%m-%d %H:%M:%S"), __type="str")
        )
    )
    response_body, response_headers = await core_prepare_response(request, response)
    return Response(content=response_body, headers=response_headers)

async def polaris_usr_get_usr_music(request: Request):
    try:
        request_info = await core_process_request(request)
        root = request_info["root"][0]
        usr_id = int(root.find("usr_id").text)
        
        db = get_db().table("polaris_score")
        scores = db.search(where("usr_id") == usr_id)
        
        # Aggregate scores to find the best per (music_id, difficulty)
        best_scores = {}
        for s in scores:
            mid = s.get("music_id")
            diff = s.get("difficulty", 0)
            key = (mid, diff)
            
            if key not in best_scores:
                best_scores[key] = {
                    "music_id": mid,
                    "difficulty": diff,
                    "score": s.get("score", 0),
                    "achievement_rate": s.get("achievement_rate", 0),
                    "clear_status": s.get("clear_status", 0),
                    "combo": s.get("combo", 0),
                    "score_rank": s.get("score_rank", 0),
                    "combo_rank": s.get("combo_rank", 0),
                    "play_count": 1,
                    "clear_count": 1 if s.get("clear_status", 0) >= 10 else 0, # Assuming >=10 is clear
                }
            else:
                entry = best_scores[key]
                entry["score"] = max(entry["score"], s.get("score", 0))
                entry["achievement_rate"] = max(entry["achievement_rate"], s.get("achievement_rate", 0))
                entry["clear_status"] = max(entry["clear_status"], s.get("clear_status", 0))
                entry["combo"] = max(entry["combo"], s.get("combo", 0))
                entry["score_rank"] = max(entry["score_rank"], s.get("score_rank", 0))
                entry["combo_rank"] = max(entry["combo_rank"], s.get("combo_rank", 0))
                entry["play_count"] += 1
                if s.get("clear_status", 0) >= 10:
                    entry["clear_count"] += 1

        music_logs = []
        for key, val in best_scores.items():
            music_logs.append(
                E.music(
                    E.music_id(val["music_id"], __type="s32"),
                    E.chart_difficulty_type(val["difficulty"], __type="s32"),
                    E.achievement_rate(val["achievement_rate"], __type="s32"),
                    E.highscore(val["score"], __type="s32"),
                    E.score_rank(val["score_rank"], __type="s32"),
                    E.maxcombo(val["combo"], __type="s32"),
                    E.combo_rank(val["combo_rank"], __type="s32"),
                    E.clear_status(val["clear_status"], __type="s32"),
                    E.play_count(val["play_count"], __type="s32"),
                    E.clear_count(val["clear_count"], __type="s32"),
                    E.perfect_clear_count(0, __type="s32"),
                    E.full_combo_count(0, __type="s32"),
                )
            )

        response = E.response(
            E.usr(
                 E.usr_music_highscore(*music_logs)
            )
        )
        response_body, response_headers = await core_prepare_response(request, response)
        return Response(content=response_body, headers=response_headers)
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return Response(status_code=500)

async def polaris_usr_save_musicscore(request: Request):
    try:
        request_info = await core_process_request(request)
        root = request_info["root"][0]
        usr_id = int(root.find("usr_id").text)
        
        logs = root.find("usr_music_play_log")
        if logs is not None:
            db = get_db().table("polaris_score")
            for log in logs.findall("music"):
                def gi(t):
                    n = log.find(t)
                    return int(n.text) if n is not None and n.text else 0
                
                mid = gi("music_id")
                diff = gi("chart_difficulty_type")
                score = gi("score")
                
                score_data = {
                    "usr_id": usr_id,
                    "music_id": mid,
                    "difficulty": diff,
                    "score": score,
                    "clear_status": gi("clear_status"),
                    "combo": gi("combo"),
                    "achievement_rate": gi("achievement_rate"),
                    "score_rank": gi("score_rank"),
                    "combo_rank": gi("combo_rank"),
                    "timestamp": int(time.time())
                }
                
                db.insert(score_data)
                print(f"polaris_usr_save_musicscore: Saved music {mid} (diff {diff}) score {score}")

        response = E.response(
            E.usr(
                E.now_date(time.strftime("%Y-%m-%d %H:%M:%S"), __type="str")
            )
        )
        response_body, response_headers = await core_prepare_response(request, response)
        return Response(content=response_body, headers=response_headers)
    except Exception as e:
        import traceback
        print(traceback.format_exc())
        return Response(status_code=500)

async def polaris_usr_checkin(request: Request):
    request_info = await core_process_request(request)
    response = E.response(
        E.usr()
    )
    response_body, response_headers = await core_prepare_response(request, response)
    return Response(content=response_body, headers=response_headers)

async def polaris_usr_checkout(request: Request):
    request_info = await core_process_request(request)
    response = E.response(E.usr())
    response_body, response_headers = await core_prepare_response(request, response)
    return Response(content=response_body, headers=response_headers)

async def polaris_usr_get_temp(request: Request):
    request_info = await core_process_request(request)
    response = E.response(E.usr())
    response_body, response_headers = await core_prepare_response(request, response)
    return Response(content=response_body, headers=response_headers)

async def polaris_usr_save_temp(request: Request):
    request_info = await core_process_request(request)
    response = E.response(E.usr())
    response_body, response_headers = await core_prepare_response(request, response)
    return Response(content=response_body, headers=response_headers)