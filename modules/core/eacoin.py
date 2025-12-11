import config

from fastapi import APIRouter, Request, Response
from tinydb import where

from core_common import core_process_request, core_prepare_response, E
from core_database import get_db

router = APIRouter(prefix="/core", tags=["eacoin"])

sessid = 0
payments = {}

@router.post("/{gameinfo}/eacoin/checkin")
async def eacoin_checkin(request: Request):
    request_info = await core_process_request(request)
    pcbid = request_info["root"].attrib["srcid"]
    cardid = request_info["root"][0].find("cardid").text

    op = get_db().table("shop").get(where("pcbid") == pcbid)
    op = {} if op is None else op

    bal = get_db().table("paseli").get(where("cardid") == cardid)
    bal = {} if bal is None else bal

    global sessid
    sessid += 1
    payments[sessid] = cardid

    response = E.response(
        E.eacoin(
            E.sequence(1, __type="s16"),
            E.acstatus(1, __type="u8"),
            E.acid(1, __type="str"),
            E.acname(op.get("opname", config.arcade), __type="str"),
            E.balance(bal.get("balance", config.paseli), __type="s32"),
            E.sessid(sessid, __type="str"),
            E.inshopcharge(1, __type="u8"),
        )
    )

    response_body, response_headers = await core_prepare_response(request, response)
    return Response(content=response_body, headers=response_headers)


@router.post("/{gameinfo}/eacoin/checkout")
async def eacoin_checkout(request: Request):
    request_info = await core_process_request(request)

    response = E.response(E.eacoin())

    response_body, response_headers = await core_prepare_response(request, response)
    return Response(content=response_body, headers=response_headers)


@router.post("/{gameinfo}/eacoin/consume")
async def eacoin_consume(request: Request):
    request_info = await core_process_request(request)
    sessid = int(request_info["root"][0].find("sessid").text)
    payment = int(request_info["root"][0].find("payment").text)

    cardid = payments.get(sessid, None)

    # fallback if server is restarted mid-round for IIDX movie or gacha purchases
    if cardid == None:
        response = E.response(
            E.eacoin(
                E.acstatus(0, __type="u8"),
                E.autocharge(0, __type="u8"),
                E.balance(config.paseli, __type="s32"),
            )
        )

        response_body, response_headers = await core_prepare_response(request, response)
        return Response(content=response_body, headers=response_headers)

    bal = get_db().table("paseli").get(where("cardid") == cardid)
    if bal == None:
        bal = {
            "cardid": cardid,
            "balance": config.paseli,
            "total_spent": 0,
        }

    new_balance = bal["balance"] - payment

    paseli_card = {
        "cardid": cardid,
        "balance": new_balance,
        "total_spent": bal["total_spent"] + payment,
    }

    response = E.response(
        E.eacoin(
            E.acstatus(0, __type="u8"),
            E.autocharge(0, __type="u8"),
            E.balance(new_balance, __type="s32"),
        )
    )

    if new_balance < 1000 or new_balance > config.paseli:
        paseli_card["balance"] = config.paseli

    get_db().table("paseli").upsert(paseli_card, where("cardid") == cardid)

    # del payments[sessid]

    response_body, response_headers = await core_prepare_response(request, response)
    return Response(content=response_body, headers=response_headers)


@router.post("/{gameinfo}/eacoin/getbalance")
async def eacoin_getbalance(request: Request):
    request_info = await core_process_request(request)

    response = E.response(
        E.eacoin(
            E.acstatus(0, __type="u8"),
            E.balance(config.paseli, __type="s32"),
        )
    )

    response_body, response_headers = await core_prepare_response(request, response)
    return Response(content=response_body, headers=response_headers)
