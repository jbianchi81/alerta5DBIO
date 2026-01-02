import { Router, Request, Response } from 'express';

import { AreaGroup } from '../models/area_group';

const router = Router();

router.get('/', async (req : Request, res : Response) => {
  const items = await AreaGroup.list(req.query);
  res.json(items);
});


// app.get('/obs/areal/area_grupos',auth.isPublic,getAreaGrupos)
// app.post('/obs/areal/area_grupos',auth.isAdmin,upsertAreaGrupos)
// app.get('/obs/areal/area_grupos/:id',auth.isPublic,getAreaGrupo)
// app.put('/obs/areal/area_grupos/:id',auth.isAdmin,upsertAreaGrupo)
// app.delete('/obs/areal/area_grupos/:id',auth.isAdmin,deleteAreaGrupo)
// app.post('/obs/areal/area_grupos/:id/members',auth.isAdmin,upsertAreaGrupoMembers)
// app.get('/obs/areal/area_grupos/:id/members',auth.isAdmin,getAreaGrupoMembers)
// app.get('/obs/areal/area_grupos/:id/members/:area_id',auth.isAdmin,getAreaGrupoMember)
