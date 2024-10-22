import os

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from fastapi.templating import Jinja2Templates
from click.testing import CliRunner

from nlds.nlds_utils.nlds_monitor import view_jobs

router = APIRouter()

template_dir = os.path.join(os.path.dirname(__file__), "../templates/")

templates = Jinja2Templates(directory=template_dir)

@router.get(
    "/",
)
async def get(
    request: Request,
):
    runner = CliRunner()
    
    result = runner.invoke(view_jobs, ["-u", "wcross"])
    
    response = {"test": "this works"}
    response = {"test": result.output}
    return templates.TemplateResponse(
        "info.html", context={"request": request, "info": response}
    )


# TODO do in monitor tools, make new pr, merge changes into this branch
# TODO move stuff out of the click function into their own function
# TODO use those functions here so I can get an easy dictionary

# no authentication required for the moment