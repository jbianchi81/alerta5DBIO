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
const router = (0, express_1.Router)();
router.get('/', (req, res) => __awaiter(void 0, void 0, void 0, function* () {
    const items = yield area_group_1.AreaGroup.list(req.query);
    res.json(items);
}));
// app.get('/obs/areal/area_grupos',auth.isPublic,getAreaGrupos)
// app.post('/obs/areal/area_grupos',auth.isAdmin,upsertAreaGrupos)
// app.get('/obs/areal/area_grupos/:id',auth.isPublic,getAreaGrupo)
// app.put('/obs/areal/area_grupos/:id',auth.isAdmin,upsertAreaGrupo)
// app.delete('/obs/areal/area_grupos/:id',auth.isAdmin,deleteAreaGrupo)
// app.post('/obs/areal/area_grupos/:id/members',auth.isAdmin,upsertAreaGrupoMembers)
// app.get('/obs/areal/area_grupos/:id/members',auth.isAdmin,getAreaGrupoMembers)
// app.get('/obs/areal/area_grupos/:id/members/:area_id',auth.isAdmin,getAreaGrupoMember)
