export async function inspect(base) {
  const plan = await (await fetch(base + '/jobs')).json();
  const response = await fetch(plan.jobs[0].url);
  return { plan, firstResult: await response.json() };
}
