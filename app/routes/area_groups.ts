import { Router, Request, Response } from 'express';

import { AreaGroup } from '../models/area_group';

import { BadRequestError, handleCrudError, NotFoundError, assertIsAdmin, getUserId } from '../custom_errors'

import Area from '../models/area'

const router = Router();

router.get('/', async (req : Request, res : Response) => {
  try {
    assertIsAdmin(req)
    const items = await AreaGroup.list(req.query);
    res.json(items);
  } catch (err: any) {
    handleCrudError(err, res)
  }
});

router.post('/', async (req : Request, res : Response) => {
  try {
    assertIsAdmin(req)
    const user_id = getUserId(req)
    const items = await AreaGroup.create(req.body, user_id);
    res.status(201).json(items);
  } catch (err: any) {
    handleCrudError(err, res)
  } 
})

router.get('/:id', async (req : Request, res : Response) => {
  try {
    const user_id = getUserId(req)
    const item = await AreaGroup.read(parseInt(req.params.id), user_id);
    if (!item) throw new NotFoundError('not found')
    res.json(item);
  } catch(e : any) {
    handleCrudError(e, res)
  }
})

router.put('/:id', async (req : Request, res : Response) => {
  try {
    assertIsAdmin(req)
    const item = await AreaGroup.update(parseInt(req.params.id), req.body);
    if (!item) throw new NotFoundError('not found');
    res.json(item);
  } catch(e : any) {
    handleCrudError(e, res)
  }
})

router.delete('/:id', async (req : Request, res : Response) => {
  try {
    assertIsAdmin(req)
    const item = await AreaGroup.delete(parseInt(req.params.id));
    if (!item) throw new NotFoundError('not found');
    res.json(item);
  } catch(e : any) {
    handleCrudError(e, res)
  }
})

router.post('/:id/access', async (req : Request, res : Response) => {
    try {
      assertIsAdmin(req)
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
      assertIsAdmin(req)
      const ugs = await AreaGroup.listAccess(parseInt(req.params.id), req.query);
      res.json(ugs);
    } catch (err: any) {
      handleCrudError(err, res)
    }
})

router.get('/:id/access/:name', async (req : Request, res : Response) => {
    try {
      assertIsAdmin(req)
      const ug = await AreaGroup.readAccess(parseInt(req.params.id), req.params.name);
      if(!ug) {
        throw new NotFoundError("No se encontró el grupo")
      }
      res.json(ug);
    } catch (err: any) {
      handleCrudError(err, res)
    }
})

router.delete('/:id/access/:name', async (req : Request, res : Response) => {
    try {
      assertIsAdmin(req)
      const ug = await AreaGroup.removeAccessOne(parseInt(req.params.id), req.params.name);
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
      const user_id = getUserId(req)
      if(!Array.isArray(req.body)) {
        throw new BadRequestError("El cuerpo de la petición debe ser un array")
      }
      const areas = req.body.map(a => {
        const area = {...a}
        area.group_id = req.params.id
        return area
      })
      const created = await Area.create(areas, user_id)
      res.json(created);
    } catch (err: any) {
      if (err.message === "GROUP_NOT_FOUND") {
        return res.status(400).json({ error: "group name not found" });
      } else {
        handleCrudError(err, res)
      }
    }
})

router.get('/:id/areas', async (req : Request, res : Response) => {
    try {
      const user_id = getUserId(req)
      const areas = await Area.list({group_id: parseInt(req.params.id), ...req.query}, undefined, user_id)
      res.json(areas);
    } catch (err: any) {
      if (err.message === "GROUP_NOT_FOUND") {
        return res.status(400).json({ error: "group name not found" });
      } else {
        handleCrudError(err, res)
      }
    }
})

router.get('/:id/areas/:area_id', async (req : Request, res : Response) => {
    try {
      const user_id = getUserId(req)
      const areas = await Area.list({group_id: parseInt(req.params.id), id: parseInt(req.params.area_id)}, undefined, user_id)
      if(!areas.length) {
        throw new NotFoundError("No se encontró el área")
      }
      res.json(areas[0]);
    } catch (err: any) {
      if (err.message === "GROUP_NOT_FOUND") {
        return res.status(400).json({ error: "group name not found" });
      } else {
        handleCrudError(err, res)
      }
    }
})


router.delete('/:id/areas/:area_id', async (req : Request, res : Response) => {
    try {
      const user_id = getUserId(req)
      const deleted = await Area.delete({group_id: parseInt(req.params.id), id: parseInt(req.params.area_id)}, user_id)
      if(!deleted.length) {
        throw new NotFoundError("No se encontró el área")
      }
      res.json(deleted[0]);
    } catch (err: any) {
      if (err.message === "GROUP_NOT_FOUND") {
        return res.status(400).json({ error: "group name not found" });
      } else {
        handleCrudError(err, res)
      }
    }
})

export default router