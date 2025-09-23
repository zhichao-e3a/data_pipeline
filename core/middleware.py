from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.trustedhost import TrustedHostMiddleware

def install_middleware(app: FastAPI) -> None:

    app.add_middleware(
        CORSMiddleware,
        allow_origin_regex=r".*",
        allow_origins=[
            "http://localhost:8501",
            "http://127.0.0.1:8501",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=[
            "aibeta.e3ahealth.com",
            "localhost",
            "127.0.0.1",
            "[::1]",
            "*.local"
        ]
    )
