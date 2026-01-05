import { Router, Request, Response } from 'express';

import { AreaGroup } from '../models/area_group';

import { BadRequestError, handleCrudError, NotFoundError } from '../custom_errors'

const router = Router();

router.get('/', async (req : Request, res : Response) => {
  const items = await AreaGroup.list(req.query);
  res.json(items);
});

router.post('/', async (req : Request, res : Response) => {
  const items = await AreaGroup.create(req.body);
  res.status(201).json(items);
})

router.get('/:id', async (req : Request, res : Response) => {
  const item = await AreaGroup.read(parseInt(req.params.id));
  if (!item) return res.status(404).json({ error: 'not found' });
  res.json(item);
})

router.put('/:id', async (req : Request, res : Response) => {
  const item = await AreaGroup.update(parseInt(req.params.id), req.body);
  if (!item) return res.status(404).json({ error: 'not found' });
  res.json(item);
})

router.delete('/:id', async (req : Request, res : Response) => {
  const item = await AreaGroup.delete(parseInt(req.params.id));
  if (!item) return res.status(404).json({ error: 'not found' });
  res.json(item);
})

router.post('/:id/access', async (req : Request, res : Response) => {
    try {
      const ugs = await AreaGroup.grantAccess(parseInt(req.params.id), req.body);
      res.json(ugs);
    } catch (err: any) {
      if (err.message === "GROUP_NOT_FOUND") {
        return res.status(400).json({ error: "group name not found" });
      } else {
        handleCrudError(err, res)
      }
    }
})

router.get('/:id/access', async (req : Request, res : Response) => {
    try {
      const ugs = await AreaGroup.listAccess(parseInt(req.params.id), req.query);
      res.json(ugs);
    } catch (err: any) {
      handleCrudError(err, res)
    }
})

router.get('/:id/access/:name', async (req : Request, res : Response) => {
    try {
      const ug = await AreaGroup.readAccess(parseInt(req.params.id), req.params.name);
      if(!ug) {
        throw new NotFoundError("No se encontró el grupo")
      }
      res.json(ug);
    } catch (err: any) {
      handleCrudError(err, res)
    }
})

router.post('/:id/areas', async (req : Request, res : Response) => {
    try {
      if(!Array.isArray(req.body)) {
        throw new BadRequestError("El cuerpo de la petición debe ser un array")
      }
      const areas = req.body.map(a => {
        const area = {...a}
        area.group_id = req.params.id
      })
      const created = await crud_areas.create(areas)
      res.json(created);
    } catch (err: any) {
      if (err.message === "GROUP_NOT_FOUND") {
        return res.status(400).json({ error: "group name not found" });
      } else {
        handleCrudError(err, res)
      }
    }
})

// app.get('/obs/areal/area_grupos/:id/members',auth.isAdmin,getAreaGrupoMembers)
// app.get('/obs/areal/area_grupos/:id/members/:area_id',auth.isAdmin,getAreaGrupoMember)
