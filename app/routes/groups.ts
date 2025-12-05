import { Router, Request, Response } from 'express';

import { Group } from '../models/group';
import { UserGroup } from "../models/user_group";
import { RedGroup } from "../models/red_group_access";

const router = Router();

router.get('/', async (req : Request, res : Response) => {
  const items = await Group.list(req.query);
  res.json(items);
});

router.get('/:name', async (req : Request, res : Response) => {
  const item = await Group.read(req.params.name);
  if (!item) return res.status(404).json({ error: 'not found' });
  res.json(item);
});

router.post('/', async (req : Request, res : Response) => {
  const created = await Group.create(req.body);
  res.status(201).json(created);
});

// router.put('/:name', async (req : Request, res : Response) => {
//   const updated = await Group.update(req.params.name, req.body);
//   if (!updated) return res.status(404).json({ error: 'not found' });
//   res.json(updated);
// });

router.delete('/:name', async (req : Request, res : Response) => {
  const deleted = await Group.delete(req.params.name);
  if (!deleted) return res.status(404).json({ error: 'not found' });
  res.json(deleted);
});

// PUT /groups/:id/members  (assign membership array)
router.put("/:group_name/members", async (req : Request, res : Response) => {
  try {
    const members = await UserGroup.assign(req.params.group_name, req.body);
    res.json(members);
  } catch (err: any) {
    if (err.message === "USER_NOT_FOUND") {
      return res.status(400).json({ error: "user id not found" });
    }
    throw err;
  }
});

router.post("/:group_name/members", async (
  req : Request<{ group_name: string }, any, { user_name: string }[]>,
  res : Response) => {
  try {
    const members = await UserGroup.add(req.params.group_name, req.body);
    res.json(members);
  } catch (err: any) {
    if (err.message === "USER_NOT_FOUND") {
      return res.status(400).json({ error: "user name not found" });
    }
    throw err;
  }
});

// GET /groups/:id/members  (list)
router.get("/:group_name/members", async (req : Request, res : Response) => {
  const list = await UserGroup.list(req.params.group_name);
  res.json(list);
});

// GET /groups/:group_id/members/:user_name
router.get("/:group_name/members/:user_name", async (req : Request, res : Response) => {
  
  const user_name = req.params.user_name;

  const member = await UserGroup.read(req.params.group_name, undefined, user_name);
  if (!member) return res.status(404).json({ error: "not found" });

  res.json(member);
});

// DELETE /groups/:group_id/members/:user_name
router.delete("/:group_name/members/:user_name", async (req : Request, res : Response) => {
  
  const user_name = req.params.user_name;

  const deleted = await UserGroup.delete(req.params.group_name, undefined, user_name);
  if (!deleted) return res.status(404).json({ error: "not found" });

  res.json(deleted);
});

// redes
router.put("/:group_name/redes", async(req : Request, res : Response) => {
  try {
    const redes = await RedGroup.add(req.params.group_name, req.body);
    res.json(redes);
  } catch (err: any) {
    if (err.message === "RED_NOT_FOUND") {
      return res.status(400).json({ error: "red id not found" });
    }
    throw err;
  }
});

router.post("/:group_name/redes", async (
  req : Request,
  res : Response) => {
  try {
    const redes = await RedGroup.add(req.params.group_name, req.body);
    res.json(redes);
  } catch (err: any) {
    if (err.message === "RED_NOT_FOUND") {
      return res.status(400).json({ error: "red id not found" });
    }
    throw err;
  }
});

// GET /groups/:id/redes  (list)
router.get("/:group_name/redes", async (req : Request, res : Response) => {
  const list = await RedGroup.list(req.params.group_name);
  res.json(list);
});

// GET /groups/:group_id/members/:user_id
router.get("/:group_name/redes/:red_id", async (req : Request, res : Response) => {
  
  const red_id = Number(req.params.red_id);

  const red = await RedGroup.read(req.params.group_name, red_id);
  if (!red) return res.status(404).json({ error: "not found" });

  res.json(red);
});

// DELETE /groups/:group_id/redes/:red_id
router.delete("/:group_name/redes/:red_id", async (req : Request, res : Response) => {
  
  const red_id = Number(req.params.red_id);

  const deleted = await RedGroup.delete(req.params.group_name, red_id);
  if (!deleted) return res.status(404).json({ error: "not found" });

  res.json(deleted);
});

export default router;
