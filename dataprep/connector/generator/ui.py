from typing import Any, Dict, Optional, Generator, Tuple
import json

from ipywidgets import (
    HTML,
    Box,
    Button,
    Dropdown,
    GridspecLayout,
    HBox,
    Layout,
    Output,
    RadioButtons,
    Text,
    Textarea,
    VBox,
)

from .generator import ConfigGenerator


class ConfigGeneratorUI:
    grid: GridspecLayout
    box_layout: Layout
    textbox_layout: Layout
    item_layout: Layout
    cg_backend: ConfigGenerator

    # UI Boxes
    request_type: Dropdown
    url_area: Text
    params_box: Textarea
    authtype_box: RadioButtons
    authparams_box: Textarea
    pagtype_box: RadioButtons
    pagparams_box: Textarea

    def __init__(self, existing: Optional[Dict[str, Any]] = None, save_file_name: Optional[str] = 'tmp.json') -> None:
        self.box_layout = Layout(
            overflow="scroll hidden",
            border="1px solid black",
            width="900px",
            height="",
            flex_flow="row",
            display="flex",
        )
        self.textbox_layout = Layout(
            overflow="scroll hidden",
            border="1px solid black",
            width="10px",
            height="",
            flex_flow="row",
            display="flex",
        )
        self.item_layout = Layout(height="200px", min_width="40px")
        self.cg_backend = ConfigGenerator(existing)
        self.save_file_name = save_file_name
        self.make_grid()

    def make_title(self) -> HBox:
        title = HTML(
            value="""<h1 style="font-family:Raleway, sans-serif; color:#444;margin:0px; padding:10px"><b>Follow these steps to generate your config file</b></h1>""",
            layout=Layout(width="925px"),
        )

        title_box = HBox([title])
        return title_box

    def make_url(self) -> VBox:
        self.url_area = Text(
            disabled=False, placeholder="input requested url", layout={"width": "100%"}
        )
        self.request_type = Dropdown(
            options=["GET", "POST", "PUT"],
            value="GET",
            disabled=False,
            layout={"width": "max-content"},
        )

        items = [self.url_area, self.request_type]
        carousel_1 = Box(children=items, layout=self.box_layout)
        request_label = HTML(
            value='<h3 style="background-color:#E8E8E8; background-size: 100px; "><span style="color: #ff0000">1.</span> API URL</h3>',
            layout=Layout(width="600px"),
        )
        explain_label_url = HTML(
            value='<p>Every web API has a URL that you can use to access its data (e.g. the URL for YouTube search API is "https://www.googleapis.com/youtube/v3/search").</p>',
            layout=Layout(width="900px"),
        )
        request_box = VBox([request_label, explain_label_url, carousel_1])
        return request_box

    def make_req_param(self) -> VBox:
        self.params_box = Textarea(
            placeholder="Please separate key and value by ':' ; while each key-value pair needs to be separated by ',' (e.g. name:abcdefg, date:2019-12-12)",
            layout={"width": "100%"},
        )

        params_label = HTML(
            value='<h3 style="background-color:#E8E8E8; background-size: 100px; "><span style="color: #ff0000">2.</span> Request Parameters</h3>',
            layout=Layout(width="600px"),
        )

        carousel_2 = Box(children=[self.params_box], layout=self.box_layout)
        param_box = VBox([params_label, carousel_2])
        return param_box

    def make_auth(self) -> VBox:
        self.authtype_box = RadioButtons(
            options=["No Authentication", "OAuth2", "QueryParam", "Bearer"],
            layout={"width": "max-content"},  # If the items' names are long
            description="",
            style={"description_width": "initial"},
            disabled=False,
        )

        auth_label = HTML(
            value='<h3 style="background-color:#E8E8E8; background-size: 100px; "><span style="color: #ff0000">3.</span> Authentication <span style="font-size: 14px"><i>(some APIs require authentication)</i> </span></span></h3>',
            layout=Layout(width="600px"),
        )

        expl = HTML(
            value="<p>Supported authentication methods are: HTTP authentication (Basic, Bearer, etc), API key as a header or query parameter. Choose one of the authentication types below:</p>",
            layout=Layout(width="900px"),
        )
        self.authparams_box = Textarea(
            placeholder="Please separate authtication key and corresponding value by ':' ; while each key-value pair needs to be separated by ',' (e.g. name:abcdefg, date:2019-12-12)",
            layout={"width": "100%"},
        )

        auth_box = VBox([auth_label, expl, self.authtype_box, self.authparams_box])
        return auth_box

    def make_pag(self) -> VBox:
        self.pagtype_box = RadioButtons(
            options=["No Pagination", "offset", "seek"],
            layout={"width": "max-content"},  # If the items' names are long
            description="",
            style={"description_width": "initial"},
            disabled=False,
        )
        pag_label = HTML(
            value='<h3 style="background-color:#E8E8E8; background-size: 100px; "><span style="color: #ff0000">4.</span> Pagination</h3>',
            layout=Layout(width="600px"),
        )
        pag_expl = HTML(
            value="<p>On some sites, a user query might match millions of records. Thus, APIs paginate the results to make sure responses are easier to handle. Some APIs posit a fixed or specifyable number of items per page, here you need to provide max number of results per page. Choose one of the authentication types below:</p>",
            layout=Layout(width="900px"),
        )
        self.pagparams_box = Textarea(
            placeholder="Please separate pagination key and corresponding value by ':' ; while each key-value pair needs to be separated by ',' (e.g. name:abcdefg, date:2019-12-12)",
            layout={"width": "100%"},
        )
        pag_box = VBox([pag_label, pag_expl, self.pagtype_box, self.pagparams_box])
        return pag_box

    def make_result(self) -> VBox:
        def on_button_clicked(b):
            if self.save_file_name is None:
                self.save_file_name = 'tmp.json'
            file = open(self.save_file_name, "w")
            file.write(self.result.value)
            file.close()
            with saved_tooltip:
                print("Saved to %s!" % self.save_file_name)
            

        self.result = Textarea(
            value="",
            placeholder="",
            description="",
            disabled=False,
            layout={"width": "90%"},
        )

        save_button = Button(
            description="Save Results", layout=Layout(width="15%", height="35px")
        )
        save_button.style.button_color = "lightblue"
        save_button.style.font_weight = "740"
        save_button.on_click(on_button_clicked)

        saved_tooltip = Output(layout={"width": "initial"})
        carousel_text = VBox([self.result, save_button, saved_tooltip])
        result_labels = HTML(
            value='<h4 style="font-family:Raleway, sans-serif; color:#444; font-weight:740">Results</h4>',
            layout=Layout(width="325px"),
        )
        send_request_expl = HTML(
            value='<h4 style="font-family:Raleway, sans-serif; color:#444;margin:0px; padding:10px">To send API url request and review results, please click on: </h4>',
            layout=Layout(width="1000px"),
        )
        send_request_button = Button(
            description="Send Request", layout=Layout(width="15%", height="35px")
        )
        send_request_button.style.button_color = "lightgreen"
        send_request_button.style.font_weight = "740"
        send_request_button.on_click(self.on_send_request)
        request_and_review_label = HTML(
            value='<h3 style="background-color:#E8E8E8; background-size: 100px; "><span style="color: #ff0000">5.</span> Send request && Review results</h3>',
            layout=Layout(width="600px"),
        )
        result_box = VBox(
            [
                request_and_review_label,
                send_request_expl,
                send_request_button,
                result_labels,
                carousel_text,
            ]
        )
        return result_box

    def make_grid(self) -> None:
        self.grid = GridspecLayout(20, 4, height="1600px", width="100Z%")
        self.grid[:1, :] = self.make_title()
        self.grid[1:3, :] = self.make_url()
        self.grid[3:6, :] = self.make_req_param()
        self.grid[6:10, :] = self.make_auth()
        self.grid[10:14, :] = self.make_pag()
        self.grid[14:, :] = self.make_result()

    def on_send_request(self, b: Any) -> None:
        params_value = dict(pairs(self.params_box.value))

        pagparams = None
        if self.pagtype_box.value != "No Pagination":
            pagparams = dict(pairs(self.pagparams_box.value))
            pagparams["type"] = self.pagtype_box.value
            pagparams["maxCount"] = int(pagparams["maxCount"])

        authparams = dict(pairs(self.authparams_box.value))

        self.dict_res = {
            "url": self.url_area.value,
            "method": self.request_type.value,
            "params": params_value,
            "pagination": pagparams,
            "authorization": (self.authtype_box.value, authparams)
            if self.authtype_box.value != "No Authentication"
            else None,
        }
        self.cg_backend.add_example(self.dict_res)
        self.result.value = (
            json.dumps(self.cg_backend.config.config.to_value(), indent=4)
        )

    def display(self) -> None:
        display(self.grid)


def pairs(input: str) -> Generator[Tuple[str, str], None, None]:
    if not input.strip():
        return

    for pair in input.split(","):

        x, *y = pair.split(":", maxsplit=1) 
        if len(y) == 0:
            raise ValueError(f"Cannot parse pair {pair}")

        yield x.strip(), y[0].strip()

