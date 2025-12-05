"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const group_1 = require("../models/group");
const user_group_1 = require("../models/user_group");
const red_group_access_1 = require("../models/red_group_access");
const router = (0, express_1.Router)();
router.get('/', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const items = yield group_1.Group.list(req.query);
    res.json(items);
}));
router.get('/:name', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const item = yield group_1.Group.read(req.params.name);
    if (!item)
        return res.status(404).json({ error: 'not found' });
    res.json(item);
}));
router.post('/', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const created = yield group_1.Group.create(req.body);
    res.status(201).json(created);
}));
// router.put('/:name', async (req : Request, res : Response) => {
//   const updated = await Group.update(req.params.name, req.body);
//   if (!updated) return res.status(404).json({ error: 'not found' });
//   res.json(updated);
// });
router.delete('/:name', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const deleted = yield group_1.Group.delete(req.params.name);
    if (!deleted)
        return res.status(404).json({ error: 'not found' });
    res.json(deleted);
}));
// PUT /groups/:id/members  (assign membership array)
router.put("/:group_name/members", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const members = yield user_group_1.UserGroup.assign(req.params.group_name, req.body);
        res.json(members);
    }
    catch (err) {
        if (err.message === "USER_NOT_FOUND") {
            return res.status(400).json({ error: "user id not found" });
        }
        throw err;
    }
}));
router.post("/:group_name/members", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const members = yield user_group_1.UserGroup.add(req.params.group_name, req.body);
        res.json(members);
    }
    catch (err) {
        if (err.message === "USER_NOT_FOUND") {
            return res.status(400).json({ error: "user name not found" });
        }
        throw err;
    }
}));
// GET /groups/:id/members  (list)
router.get("/:group_name/members", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const list = yield user_group_1.UserGroup.list(req.params.group_name);
    res.json(list);
}));
// GET /groups/:group_id/members/:user_name
router.get("/:group_name/members/:user_name", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const user_name = req.params.user_name;
    const member = yield user_group_1.UserGroup.read(req.params.group_name, undefined, user_name);
    if (!member)
        return res.status(404).json({ error: "not found" });
    res.json(member);
}));
// DELETE /groups/:group_id/members/:user_name
router.delete("/:group_name/members/:user_name", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const user_name = req.params.user_name;
    const deleted = yield user_group_1.UserGroup.delete(req.params.group_name, undefined, user_name);
    if (!deleted)
        return res.status(404).json({ error: "not found" });
    res.json(deleted);
}));
// redes
router.put("/:group_name/redes", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const redes = yield red_group_access_1.RedGroup.add(req.params.group_name, req.body);
        res.json(redes);
    }
    catch (err) {
        if (err.message === "RED_NOT_FOUND") {
            return res.status(400).json({ error: "red id not found" });
        }
        throw err;
    }
}));
router.post("/:group_name/redes", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const redes = yield red_group_access_1.RedGroup.add(req.params.group_name, req.body);
        res.json(redes);
    }
    catch (err) {
        if (err.message === "RED_NOT_FOUND") {
            return res.status(400).json({ error: "red id not found" });
        }
        throw err;
    }
}));
// GET /groups/:id/redes  (list)
router.get("/:group_name/redes", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const list = yield red_group_access_1.RedGroup.list(req.params.group_name);
    res.json(list);
}));
// GET /groups/:group_id/members/:user_id
router.get("/:group_name/redes/:red_id", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const red_id = Number(req.params.red_id);
    const red = yield red_group_access_1.RedGroup.read(req.params.group_name, red_id);
    if (!red)
        return res.status(404).json({ error: "not found" });
    res.json(red);
}));
// DELETE /groups/:group_id/redes/:red_id
router.delete("/:group_name/redes/:red_id", (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const red_id = Number(req.params.red_id);
    const deleted = yield red_group_access_1.RedGroup.delete(req.params.group_name, red_id);
    if (!deleted)
        return res.status(404).json({ error: "not found" });
    res.json(deleted);
}));
exports.default = router;
