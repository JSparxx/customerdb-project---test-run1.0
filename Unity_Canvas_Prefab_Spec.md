# Unity Canvas Prefab — CRUD UI (Spec)

Canvas (Screen Space - Overlay)
└── Panel
    ├── RowTop
    │   ├── BtnCreate
    │   ├── BtnRead
    │   ├── BtnUpdate
    │   └── BtnDelete
    ├── Fields
    │   ├── LabelID + InputID (TMP_InputField) [ReadOnly]
    │   ├── LabelFirst + InputFirst
    │   ├── LabelLast + InputLast
    │   └── LabelEmail + InputEmail
    └── Toast
        └── ToastText (TextMeshProUGUI)

Wire these in UIManager.cs:
- inputFirst ← InputFirst
- inputLast  ← InputLast
- inputEmail ← InputEmail
- inputId    ← InputID
- toast      ← Toast
- toastText  ← ToastText

Buttons → UIManager methods:
- BtnCreate → BtnCreate()
- BtnRead   → BtnRead()
- BtnUpdate → BtnUpdate()
- BtnDelete → BtnDelete()

Add ApiClient (set baseUrl) and TapToPlace (assign placePrefab) in scene.
