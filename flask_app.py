from flask import redirect

from app import app


@app.route("/tgodispatchtool")
def dispatch_tool_alias():
    return redirect("/", code=302)


application = app


if __name__ == "__main__":
    app.run()
