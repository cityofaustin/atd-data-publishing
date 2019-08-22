import knackpy
import requests

import _setpath
from config.secrets import *
from config.knack.config import SR_ASSET_ASSIGNER as cfg

import agolutil
import argutil


def cli_args():
    parser = argutil.get_parser(
        "sr_asset_assign.py",
        "Link 311 service request to nearby assset record.",
        "device_type",
        "app_name",
    )

    args = parser.parse_args()

    return args


def knackpy_wrapper(cfg, auth, obj=None, filters=None):
    return knackpy.Knack(
        obj=obj,
        scene=cfg["scene"],
        view=cfg["view"],
        ref_obj=cfg["ref_obj"],
        app_id=auth["app_id"],
        api_key=auth["api_key"],
        filters=filters,
        page_limit=10000,
    )


def get_token(email, pw, app_id):
    data = {"email": email, "password": pw}
    url = f"https://api.knack.com/v1/applications/{app_id}/session"
    headers = {"Content-Type": "application/json"}
    res = requests.post(url, headers=headers, json=data)
    res.raise_for_status()
    return res.json()["session"]["user"]["token"]


def form_submit(token, app_id, record_id, asset_type, asset_field_id, asset_id):
    url = (
        f"https://api.knack.com/v1/pages/scene_428/views/view_2367/records/{record_id}"
    )

    headers = {"X-Knack-Application-Id": app_id, "Authorization": token}

    data = {"field_1649": asset_type, asset_field_id: asset_id}  # TODO: move to config

    res = requests.put(url, headers=headers, json=data)
    res.raise_for_status()
    return res


def get_params(layer_config):
    """base params for AGOL query request"""
    params = {
        "f": "json",
        "outFields": "*",
        "geometry": None,
        "geomtryType": "esriGeometryPoint",
        "returnGeometry": False,
        "spatialRel": "esriSpatialRelIntersects",
        "inSR": 2277,
        "geometryType": "esriGeometryPoint",
        "distance": None,
        "units": None,
    }

    for param in layer_config:
        if param in params:
            params[param] = layer_config[param]

    return params


def get_geom(config, record):
    x = record.get(config["tmc_issues"]["x_field"])
    y = record.get(config["tmc_issues"]["y_field"])

    if not x and y:
        # this should not happen, because the source knack view excludes records without x and y
        raise Exception("No geometry found for {}".format(record))
    else:
        return [y, x]


def asset_filter(field, value):
    return {
        "match": "or",
        "rules": [{"field": f"{field}", "operator": "is", "value": f"{value}"}],
    }


def no_asset_found_payload(record_id, field_id):
    return {"id": record_id, field_id: "no_asset_found"}


def update_record(payload, auth, obj):
    return knackpy.record(
        payload,
        obj_key=obj,
        app_id=auth["app_id"],
        api_key=auth["api_key"],
        method="update",
    )


def main():
    args = cli_args()
    asset_type = args.device_type
    app_name = args.app_name

    layer = cfg[asset_type]["layer"]
    disp_name = cfg[asset_type]["display_name"]
    asset_field_id = cfg["tmc_issues"]["connection_field_keys"][asset_type]

    tmc_issues = knackpy_wrapper(cfg["tmc_issues"], KNACK_CREDENTIALS[app_name])

    if not tmc_issues.data:
        return 0

    # get token once to create one session per script run
    token = get_token(
        KNACK_API_USER_CREDS["email"],
        KNACK_API_USER_CREDS["password"],
        KNACK_CREDENTIALS[app_name]["app_id"],
    )

    for record in tmc_issues.data:

        params = get_params(layer)

        params["geometry"] = get_geom(cfg, record)

        res = agolutil.point_in_poly(layer["service_name"], layer["layer_id"], params)

        # we have to manually check for response errors. The API returns `200` regardless
        if "error" in res:
            raise Exception(res)
        
        if not res.get("features"):
            # no nearby asset found
            payload = no_asset_found_payload(
                record["id"], cfg["tmc_issues"]["assign_status_field_id"]
            )

            res = update_record(
                payload, KNACK_CREDENTIALS[app_name], cfg["tmc_issues"]["ref_obj"][0]
            )
            
            continue

        elif len(res["features"]) != 1:
            # ignore record if multiple features found
            continue

        asset_id = res["features"][0]["attributes"].get(layer["primary_key"])
        filters = asset_filter(cfg[asset_type]["primary_key"], asset_id)

        asset = knackpy_wrapper(
            cfg[asset_type], KNACK_CREDENTIALS[app_name], filters=filters
        )

        if len(asset.data) > 1:
            raise Exception("More than one Knack asset found for given asset id.")
        elif len(asset.data) == 0:
            """
            This should never happen. All GIS features are created from Knack data,
            so there should always be a corresponding Knack asset.
            """
            raise Exception("No corresponding Knack asset found for GIS feature.")
        else:
            asset_id = asset.data[0]["id"]

        res = form_submit(
            token,
            KNACK_CREDENTIALS[app_name]["app_id"],
            record["id"],
            disp_name,
            asset_field_id,
            asset_id,
        )

    return len(tmc_issues.data)

if __name__ == "__main__":
    main()
