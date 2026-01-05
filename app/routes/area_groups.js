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
const area_group_1 = require("../models/area_group");
const custom_errors_1 = require("../custom_errors");
const router = (0, express_1.Router)();
router.get('/', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const items = yield area_group_1.AreaGroup.list(req.query);
    res.json(items);
}));
router.post('/', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const items = yield area_group_1.AreaGroup.create(req.body);
    res.status(201).json(items);
}));
router.get('/:id', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const item = yield area_group_1.AreaGroup.read(parseInt(req.params.id));
    if (!item)
        return res.status(404).json({ error: 'not found' });
    res.json(item);
}));
router.put('/:id', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const item = yield area_group_1.AreaGroup.update(parseInt(req.params.id), req.body);
    if (!item)
        return res.status(404).json({ error: 'not found' });
    res.json(item);
}));
router.delete('/:id', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const item = yield area_group_1.AreaGroup.delete(parseInt(req.params.id));
    if (!item)
        return res.status(404).json({ error: 'not found' });
    res.json(item);
}));
router.post('/:id/access', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const ugs = yield area_group_1.AreaGroup.grantAccess(parseInt(req.params.id), req.body);
        res.json(ugs);
    }
    catch (err) {
        if (err.message === "GROUP_NOT_FOUND") {
            return res.status(400).json({ error: "group name not found" });
        }
        else {
            (0, custom_errors_1.handleCrudError)(err, res);
        }
    }
}));
router.get('/:id/access', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const ugs = yield area_group_1.AreaGroup.listAccess(parseInt(req.params.id), req.query);
        res.json(ugs);
    }
    catch (err) {
        (0, custom_errors_1.handleCrudError)(err, res);
    }
}));
router.get('/:id/access/:name', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const ug = yield area_group_1.AreaGroup.readAccess(parseInt(req.params.id), req.params.name);
        if (!ug) {
            throw new custom_errors_1.NotFoundError("No se encontró el grupo");
        }
        res.json(ug);
    }
    catch (err) {
        (0, custom_errors_1.handleCrudError)(err, res);
    }
}));
// app.get('/obs/areal/area_grupos/:id/members',auth.isAdmin,getAreaGrupoMembers)
// app.get('/obs/areal/area_grupos/:id/members/:area_id',auth.isAdmin,getAreaGrupoMember)
