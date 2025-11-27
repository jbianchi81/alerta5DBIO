import { Router, Request, Response } from 'express';

import { Group } from '../models/group';
import { UserGroup } from "../models/user_group";

const router = Router();

router.get('/', async (req : Request, res : Response) => {
  const items = await Group.list();
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
  req : Request<{ group_name: string }, any, { user_id: number }[]>,
  res : Response) => {
  try {
    const members = await UserGroup.add(req.params.group_name, req.body);
    res.json(members);
  } catch (err: any) {
    if (err.message === "USER_NOT_FOUND") {
      return res.status(400).json({ error: "user id not found" });
    }
    throw err;
  }
});

// GET /groups/:id/members  (list)
router.get("/:group_name/members", async (req : Request, res : Response) => {
  const list = await UserGroup.list(req.params.group_name);
  res.json(list);
});

// GET /groups/:group_id/members/:user_id
router.get("/:group_name/members/:user_id", async (req : Request, res : Response) => {
  
  const user_id = Number(req.params.user_id);

  const member = await UserGroup.read(req.params.group_name, user_id);
  if (!member) return res.status(404).json({ error: "not found" });

  res.json(member);
});

// DELETE /groups/:group_id/members/:user_id
router.delete("/:group_name/members/:user_id", async (req : Request, res : Response) => {
  
  const user_id = Number(req.params.user_id);

  const deleted = await UserGroup.delete(req.params.group_name, user_id);
  if (!deleted) return res.status(404).json({ error: "not found" });

  res.json(deleted);
});

export default router;
