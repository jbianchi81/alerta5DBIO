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
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const express_1 = require("express");
const area_group_1 = require("../models/area_group");
const custom_errors_1 = require("../custom_errors");
const area_1 = __importDefault(require("../models/area"));
const router = (0, express_1.Router)();
router.get('/', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        (0, custom_errors_1.assertIsAdmin)(req);
        const items = yield area_group_1.AreaGroup.list(req.query);
        res.json(items);
    }
    catch (err) {
        (0, custom_errors_1.handleCrudError)(err, res);
    }
}));
router.post('/', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        (0, custom_errors_1.assertIsAdmin)(req);
        const user_id = (0, custom_errors_1.getUserId)(req);
        const items = yield area_group_1.AreaGroup.create(req.body, user_id);
        res.status(201).json(items);
    }
    catch (err) {
        (0, custom_errors_1.handleCrudError)(err, res);
    }
}));
router.get('/:id', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const user_id = (0, custom_errors_1.getUserId)(req);
        const item = yield area_group_1.AreaGroup.read(parseInt(req.params.id), user_id);
        if (!item)
            throw new custom_errors_1.NotFoundError('not found');
        res.json(item);
    }
    catch (e) {
        (0, custom_errors_1.handleCrudError)(e, res);
    }
}));
router.put('/:id', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        (0, custom_errors_1.assertIsAdmin)(req);
        const item = yield area_group_1.AreaGroup.update(parseInt(req.params.id), req.body);
        if (!item)
            throw new custom_errors_1.NotFoundError('not found');
        res.json(item);
    }
    catch (e) {
        (0, custom_errors_1.handleCrudError)(e, res);
    }
}));
router.delete('/:id', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        (0, custom_errors_1.assertIsAdmin)(req);
        const item = yield area_group_1.AreaGroup.delete(parseInt(req.params.id));
        if (!item)
            throw new custom_errors_1.NotFoundError('not found');
        res.json(item);
    }
    catch (e) {
        (0, custom_errors_1.handleCrudError)(e, res);
    }
}));
router.post('/:id/access', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        (0, custom_errors_1.assertIsAdmin)(req);
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
        (0, custom_errors_1.assertIsAdmin)(req);
        const ugs = yield area_group_1.AreaGroup.listAccess(parseInt(req.params.id), req.query);
        res.json(ugs);
    }
    catch (err) {
        (0, custom_errors_1.handleCrudError)(err, res);
    }
}));
router.get('/:id/access/:name', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        (0, custom_errors_1.assertIsAdmin)(req);
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
router.delete('/:id/access/:name', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        (0, custom_errors_1.assertIsAdmin)(req);
        const ug = yield area_group_1.AreaGroup.removeAccessOne(parseInt(req.params.id), req.params.name);
        if (!ug) {
            throw new custom_errors_1.NotFoundError("No se encontró el grupo");
        }
        res.json(ug);
    }
    catch (err) {
        (0, custom_errors_1.handleCrudError)(err, res);
    }
}));
router.post('/:id/areas', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const user_id = (0, custom_errors_1.getUserId)(req);
        if (!Array.isArray(req.body)) {
            throw new custom_errors_1.BadRequestError("El cuerpo de la petición debe ser un array");
        }
        const areas = req.body.map(a => {
            const area = Object.assign({}, a);
            area.group_id = req.params.id;
            return area;
        });
        const created = yield area_1.default.create(areas, user_id);
        res.json(created);
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
router.get('/:id/areas', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const user_id = (0, custom_errors_1.getUserId)(req);
        const areas = yield area_1.default.list(Object.assign({ group_id: parseInt(req.params.id) }, req.query), undefined, user_id);
        res.json(areas);
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
router.get('/:id/areas/:area_id', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const user_id = (0, custom_errors_1.getUserId)(req);
        const areas = yield area_1.default.list({ group_id: parseInt(req.params.id), id: parseInt(req.params.area_id) }, undefined, user_id);
        if (!areas.length) {
            throw new custom_errors_1.NotFoundError("No se encontró el área");
        }
        res.json(areas[0]);
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
router.delete('/:id/areas/:area_id', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    try {
        const user_id = (0, custom_errors_1.getUserId)(req);
        const deleted = yield area_1.default.delete({ group_id: parseInt(req.params.id), id: parseInt(req.params.area_id) }, user_id);
        if (!deleted.length) {
            throw new custom_errors_1.NotFoundError("No se encontró el área");
        }
        res.json(deleted[0]);
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
exports.default = router;
