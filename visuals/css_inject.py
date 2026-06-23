

SIDE_BAR_CSS    = """
    <style>
        .element-container:has(style) {
            display: none !important;
        }
        /* 1. Ép độ rộng sidebar khi mở */
        [data-testid="stSidebar"][aria-expanded="true"] {
            min-width: 320px;
            max-width: 325px;
        }
        /* 2. No ease */
        [data-testid="stSidebar"] {
            transition: min-width 0s ease-in-out, max-width 0s ease-in-out !important;
            overflow-x: hidden !important;
        }
    </style>
    """
PAGE_FONT_CSS   = '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=Material+Symbols+Outlined:opsz,wght,FILL,GRAD@24,400,0,0" />'
POPOVER_CSS     = """
    <style>
    /* Chỉ áp dụng cho nút popover (ngoài sidebar) */
    div[data-testid="stMain"] div[data-testid="stPopover"] button p {
        color: #49618D !important;
    }
    </style>
    """
DF_SCROLL_HTML  = """
    <style>
    /* Chống tràn cuộn (overscroll) cho st.dataframe */
    div[data-testid='stDataFrame'] div {
        overscroll-behavior: contain !important;
    }
    </style>
    """
MARK_DOWN_CSS   = """
    <style>
    /* Phóng to chữ `ở_trong` (thẻ code của markdown) */
    code {
        font-family: sans-serif !important; 
        font-size: 0.95rem !important;
        font-weight: 550 !important;
        color: #49618D !important;
        background-color: transparent !important;
    }
    </style>
    """
